//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use chainpack::{ChainPackReader, Reader, RpcMessage, CponReader, RpcValue};
use chainpack::RpcMessageMetaTags;
use crate::db::Db;
use crate::Connection;
use tracing::{debug, instrument};
use bytes::Buf;

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub struct Frame {
    pub protocol: Protocol,
    pub message: RpcMessage,
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

#[derive(Clone, Debug)]
pub enum Protocol {
    ChainPack = 1,
    Cpon,
}
impl fmt::Display for Protocol {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Protocol::ChainPack => write!(fmt, "{}", "ChainPack"),
            Protocol::Cpon => write!(fmt, "{}", "Cpon"),
        }
    }
}
impl Frame {

    /// Checks if an entire message can be decoded from `src`
    pub fn check(buff: &mut Cursor<&[u8]>) -> Result<usize, Error> {
        // min RpcMessage must have at least 6 bytes
        const MIN_LEN: usize = 6;
        let buff_len = buff.get_ref().len();
        if buff_len < MIN_LEN {
            return Err(Error::Incomplete)
        }
        let mut cpk_rd = ChainPackReader::new(buff);
        let msg_len = cpk_rd.read_uint_data()? as usize;
        let header_len = buff.position() as usize;
        if buff_len < header_len + msg_len {
            return Err(Error::Incomplete)
        }
        return Ok(msg_len)
    }

    /// The message has already been validated with `check`.
    pub fn parse(buff: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let proto = buff.get_u8();
        if proto == Protocol::ChainPack as u8 {
            let mut rd = ChainPackReader::new(buff);
            let rv = rd.read()?;
            return Ok(Frame{protocol: Protocol::ChainPack, message: RpcMessage::from_rpcvalue(rv)?})
        } else if proto == Protocol::Cpon as u8 {
            let mut rd = CponReader::new(buff);
            let rv = rd.read()?;
            return Ok(Frame{protocol: Protocol::Cpon, message: RpcMessage::from_rpcvalue(rv)?})
        }
        Err(Error::from(format!("Invalid protocol: {}!", proto)))
    }
    /// Apply the `Frame` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // echo for now
        match (&self.message).create_response() {
            Ok(mut resp) => {
                resp.set_result(RpcValue::new(&format!("Method '{}' called on shvPath: {}"
                                                       , &self.message.method().unwrap_or("")
                                                       , &self.message.shv_path().unwrap_or("")
                )));
                debug!(?resp);
                // Write the response back to the client
                dst.write_frame(&Frame {protocol: self.protocol, message: resp}).await?;
                Ok(())
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }
    /// Converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{{proto:{},message:{}}}", self.protocol, self.message)
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

impl From<chainpack::ReadError> for Error {
    fn from(e: chainpack::ReadError) -> Error {
        e.msg.into()
    }
}