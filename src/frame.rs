//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use std::fmt;
use std::io::{Cursor, BufReader};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use chainpack::{ChainPackReader, Reader, RpcMessage, CponReader, RpcValue, MetaMap, ChainPackWriter, CponWriter, Writer};
use crate::db::Db;
use crate::Connection;
use tracing::{instrument};
use bytes::Buf;

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub struct Frame {
    pub protocol: Protocol,
    pub meta: MetaMap,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

#[derive(Clone, Copy, Debug)]
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
    pub fn new(protocol: Protocol, meta: MetaMap, data: Vec<u8>) -> Frame {
        Frame{ protocol, meta, data }
    }
    pub fn from_rpcmessage(protocol: Protocol, msg: &RpcMessage) -> Frame {
        let mut data = Vec::new();
        match &protocol {
            Protocol::ChainPack => {
                let mut wr = ChainPackWriter::new(&mut data);
                wr.write_value(&msg.as_rpcvalue().value()).unwrap();
            }
            Protocol::Cpon => {
                let mut wr = CponWriter::new(&mut data);
                wr.write_value(&msg.as_rpcvalue().value()).unwrap();
            }
        }
        let meta = msg.as_rpcvalue().meta().clone();
        Frame{ protocol, meta, data }
    }
    pub fn to_rpcmesage(&self) -> Result<RpcMessage, Error> {
        let mut buff = BufReader::new(&*self.data);
        let value;
        match &self.protocol {
            Protocol::ChainPack => {
                let mut rd = ChainPackReader::new(&mut buff);
                value = rd.read_value()?;
            }
            Protocol::Cpon => {
                let mut rd = CponReader::new(&mut buff);
                value = rd.read_value()?;
            }
        }
        Ok(RpcMessage::new(self.meta.clone(), value))
    }
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
        return Ok(header_len + msg_len)
    }

    /// The message has already been validated with `check`.
    pub fn parse(buff: &mut Cursor<&[u8]>, frame_len: usize) -> Result<Frame, Error> {
        // debug!("parse pos1: {}", buff.position());
        let proto = buff.get_u8();
        let protocol;
        let meta;
        if proto == Protocol::ChainPack as u8 {
            protocol = Protocol::ChainPack;
            let mut rd = ChainPackReader::new(buff);
            meta = rd.try_read_meta()?.unwrap();
        } else if proto == Protocol::Cpon as u8 {
            protocol = Protocol::Cpon;
            let mut rd = CponReader::new(buff);
            meta = rd.try_read_meta()?.unwrap();
        } else {
            return Err(Error::from(format!("Invalid protocol: {}!", proto)))
        }
        let pos = buff.position() as usize;
        // debug!("parse pos2: {}", pos);
        // debug!("parse data len: {}", (frame_len - pos));
        let data: Vec<u8> = buff.get_ref()[pos .. frame_len].into();
        return Ok(Frame{protocol, meta, data})
    }
    /// Apply the `Frame` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // echo for now
        match RpcMessage::create_response_meta(&self.meta) {
            Ok(resp_meta) => {
                /*
                resp.set_result(RpcValue::new(&format!("Method '{}' called on shvPath: {}"
                                                       , &self.message.method().unwrap_or("")
                                                       , &self.message.shv_path().unwrap_or("")
                )));
                 */
                let mut result = chainpack::rpcvalue::Map::new();
                result.insert("nonce".into(), RpcValue::new("123456"));
                let mut resp = RpcMessage::from_meta(resp_meta);
                resp.set_result(RpcValue::new(result));                // Write the response back to the client
                dst.send_frame(&Frame::from_rpcmessage(self.protocol, &resp)).await?;
                Ok(())
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }
    // Converts the frame to an "unexpected frame" error
    // pub(crate) fn to_error(&self) -> crate::Error {
    //     format!("unexpected frame: {}", self).into()
    // }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{{proto:{}, meta:{}, data len: {}}}", self.protocol, self.meta, self.data.len())
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