//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use std::fmt;
use std::io::{Cursor, BufReader};
use chainpack::{ChainPackReader, Reader, RpcMessage, CponReader, MetaMap, ChainPackWriter, CponWriter, Writer};
// use tracing::{instrument};
use bytes::Buf;

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub struct Frame {
    pub protocol: Protocol,
    pub meta: MetaMap,
    pub data: Vec<u8>,
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
    pub fn to_rpcmesage(&self) -> crate::Result<RpcMessage> {
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

    /// The message has already been validated with `check`.
    pub fn parse(buff: &[u8]) -> crate::Result<Option<(usize, Frame)>> {
        // min RpcMessage must have at least 6 bytes
        const MIN_LEN: usize = 6;
        let buff_len = buff.len();
        if buff_len < MIN_LEN {
            return Ok(None)
        }
        // debug!("parse pos1: {}", buff.position());
        let mut buff_cursor = Cursor::new(buff);
        let mut cpk_rd = ChainPackReader::new(&mut buff_cursor);
        let msg_len = cpk_rd.read_uint_data()? as usize;
        let header_len = buff_cursor.position() as usize;
        let frame_len = header_len + msg_len;
        if buff_len < frame_len {
            return Ok(None)
        }
        let proto = buff_cursor.get_u8();
        let protocol;
        let meta;
        if proto == Protocol::ChainPack as u8 {
            protocol = Protocol::ChainPack;
            let mut rd = ChainPackReader::new(&mut buff_cursor);
            meta = rd.try_read_meta()?.unwrap();
        } else if proto == Protocol::Cpon as u8 {
            protocol = Protocol::Cpon;
            let mut rd = CponReader::new(&mut buff_cursor);
            meta = rd.try_read_meta()?.unwrap();
        } else {
            return Err(format!("Invalid protocol: {}!", proto).into())
        }
        let pos = buff_cursor.position() as usize;
        // debug!("parse pos2: {}", pos);
        // debug!("parse data len: {}", (frame_len - pos));
        let data: Vec<u8> = buff[pos .. frame_len].into();
        return Ok(Some((frame_len, Frame { protocol, meta, data })))
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{{proto:{}, meta:{}, data len: {}}}", self.protocol, self.meta, self.data.len())
    }
}

