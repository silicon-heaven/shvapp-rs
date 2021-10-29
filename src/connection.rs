use crate::frame::{self, Frame, Protocol};
use crate::client::{Client};
use bytes::{Buf, BytesMut};
use std::io::{Cursor};
use std::sync::mpsc::channel;
use chainpack::{RpcMessage, ChainPackWriter, Writer, CponWriter, RpcValue, RpcMessageMetaTags};
use log::{debug, error};
use std::time::Duration;
use chainpack::rpcmessage::RqId;
use async_std::{
    channel::{Receiver, Sender},
    io::{stdin, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};
use futures::{select, FutureExt};


#[derive(Debug)]
pub struct Connection<'a> {
    stream_reader: BufReader<&'a TcpStream>,
    stream_writer: BufWriter<&'a TcpStream>,
    // The buffer for reading frames.
    buffer: BytesMut,
    from_client: (Sender<Frame>, Receiver<Frame>),
    to_client: (Sender<Frame>, Receiver<Frame>),
}

impl<'a> Connection<'a> {
    pub fn new(stream: &'a TcpStream) -> Connection<'a> {
        Connection {
            stream_reader: BufReader::new(stream),
            stream_writer: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
            from_client: async_std::channel::bounded(256),
            to_client: async_std::channel::bounded(256),
        }
    }
    pub fn newClient(&self, protocol: Protocol) -> Client {
        Client {
            sender: self.from_client.0.clone(),
            receiver: self.to_client.1.clone(),
            protocol
        }
    }
    pub async fn exec(&mut self) -> crate::Result<()> {
        loop {
            let mut buf: [u8; 1024] = [0; 1024];
            select! {
                n = self.stream_reader.read(&mut buf).fuse() => match n {
                    Ok(n) => {
                        debug!("{} bytes read", n);
                        self.buffer.extend_from_slice(&buf[..n]);
                        match self.receive_frame() {
                            Ok(frame) => match frame {
                                Some(frame) => {
                                    debug!("New frame from socket received");
                                    self.to_client.0.send(frame);
                                }
                                None => {
                                    // not all frame data received
                                }
                            }
                            Err(e) => {
                                error!("read frame error {}", e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("read socket error {}", e);
                    },
                },
                frame = self.from_client.1.recv().fuse() => match frame {
                    Ok(frame) => {
                        self.send_frame(&frame).await?;
                    }
                    Err(e) => {
                        error!("read frame error {}", e);
                    },
                }
            }
        }
    }
    /*
    pub async fn recv_frame(&mut self) -> crate::Result<Frame> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                if frame.data.len() < 1024 {
                    debug!("===> {}", frame.to_rpcmesage().unwrap_or(RpcMessage::default()));
                } else {
                    debug!("===> {}", frame);
                }
                return Ok(frame);
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                //return Err("connection reset by peer".into())
                return Err("connection reset by peer".into())
            }
        }
    }
    */
    fn receive_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buff_cursor = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buff_cursor) {
            Ok(frame_len) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                //let header_len = buff_cursor.position() as usize;

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                // debug!("check header len: {}", header_len);
                // debug!("check frame len: {}", frame_len);
                let result = Frame::parse(&mut buff_cursor, frame_len);
                match result {
                   Ok(frame) => {
                       self.buffer.advance(frame_len);
                       Ok(Some(frame))
                   }
                   Err(e) => {
                       // ignore rest of data in case of corrupted message
                       self.buffer.advance(frame_len);
                       return Err(e.into())
                   }
                }
                // Return the parsed frame to the caller.
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    pub async fn send_frame(&mut self, frame: &Frame) -> crate::Result<()> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        if frame.data.len() < 1024 {
            debug!("<=== {}", frame.to_rpcmesage().unwrap_or(RpcMessage::default()));
        } else {
            debug!("<=== {}", frame);
        }
        let mut meta_data = Vec::new();
        match &frame.protocol {
            Protocol::ChainPack => {
                let mut wr = ChainPackWriter::new(&mut meta_data);
                wr.write_meta(&frame.meta)?;
            }
            Protocol::Cpon => {
                let mut wr = CponWriter::new(&mut meta_data);
                wr.write_meta(&frame.meta)?;
            }
        }
        let mut header = Vec::new();
        let mut wr = ChainPackWriter::new(&mut header);
        let msg_len = 1 + meta_data.len() + frame.data.len();
        wr.write_uint_data(msg_len as u64)?;
        header.push(frame.protocol as u8);
        self.stream_writer.write_all(&header).await?;
        self.stream_writer.write_all(&meta_data).await?;
        self.stream_writer.write_all(&frame.data).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream_writer.flush().await?;
        Ok(())
    }
}
