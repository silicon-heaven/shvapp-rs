use crate::frame::{self, Frame, Protocol};
use crate::client::{Client};
use bytes::{Buf, BytesMut};
use std::io::{Cursor};
use chainpack::{ChainPackWriter, Writer, CponWriter};
use log::{debug, error};
use async_std::{
    channel::{Receiver, Sender},
    // io::{stdin, BufReader, BufWriter},
    net::{TcpStream},
    prelude::*,
    // task,
};
use futures::{select, FutureExt};
use postage::prelude::Sink;

//#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    // The buffer for reading frames.
    buffer: BytesMut,
    from_client: (Sender<Frame>, Receiver<Frame>),
    // to_client: (Sender<Frame>, Receiver<Frame>),
    to_client: (postage::broadcast::Sender<Frame>, postage::broadcast::Receiver<Frame>),
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4 * 1024),
            from_client: async_std::channel::bounded(256),
            //to_client: async_std::channel::bounded(256),
            // unfortunately, async-std channel is dispatch not broadcast
            // we have to use broadcast channel from postage crate
            to_client: postage::broadcast::channel(256),
        }
    }
    pub fn create_client(&self, protocol: Protocol) -> Client {
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
                n = self.stream.read(&mut buf).fuse() => match n {
                    Ok(n) => {
                        debug!("{} bytes read from socket", n);
                        self.buffer.extend_from_slice(&buf[..n]);
                        match self.receive_frame() {
                            Ok(frame) => match frame {
                                Some(frame) => {
                                    //debug!("New frame from socket received: {}", &frame);
                                    self.to_client.0.send(frame).await?;
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
                        debug!(target: "rpcmsg", "Frame to send from client: {}", &frame);
                        self.send_frame(&frame).await?;
                    }
                    Err(e) => {
                        error!("read frame error {}", e);
                    },
                }
            }
        }
    }
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
                       debug!(target: "rpcmsg", "<=== {}", frame);
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
        debug!(target: "rpcmsg", "===> {}", frame);
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
        self.stream.write(&header).await?;
        self.stream.write(&meta_data).await?;
        self.stream.write(&frame.data).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await?;
        Ok(())
    }
}
