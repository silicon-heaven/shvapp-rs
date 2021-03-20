use crate::frame::{self, Frame, Protocol};
use bytes::{Buf, BytesMut};
use std::io::{Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use chainpack::{RpcMessage, ChainPackWriter, Writer, CponWriter, RpcValue, RpcMessageMetaTags};
// use tracing::{debug};
use log::{debug};
use std::time::Duration;
use chainpack::rpcmessage::RqId;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
    pub protocol: Option<Protocol>,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
            protocol: None,
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
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

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
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

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
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
        self.stream.write_all(&header).await?;
        self.stream.write_all(&meta_data).await?;
        self.stream.write_all(&frame.data).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await?;
        Ok(())
    }
    pub async fn recv_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.recv_frame().await?;
        let msg = frame.to_rpcmesage()?;
        Ok(msg)
    }
    pub async fn recv_message_timeout(&mut self, timeout: Duration) -> crate::Result<RpcMessage> {
        match tokio::time::timeout(timeout, self.recv_message()).await {
            Ok(maybe_message) => {
                maybe_message
            }
            Err(e) => {
                Err(format!("Read message timeout after: {} sec - {}", timeout.as_secs(), e).into())
            }
        }
    }
    pub async fn send_message(&mut self, message: &RpcMessage) -> crate::Result<()> {
        let frame = Frame::from_rpcmessage(self.protocol.unwrap(), message);
        self.send_frame(&frame).await
    }
    pub async fn send_request(&mut self, shvpath: &str, method: &str, params: Option<RpcValue>) -> crate::Result<RqId> {
        let msg = RpcMessage::create_request(shvpath, method, params);
        let rq_id = msg.request_id().unwrap();
        self.send_message(&msg).await?;
        Ok(rq_id)
    }
    // pub async fn rpc_call_timeout(&mut self, request: &RpcMessage, timeout: Duration) -> crate::Result<RpcMessage> {
    //     let fut = async move {
    //         self.write_message(request).await?;
    //         self.read_message().await
    //     };
    //     match tokio::time::timeout(timeout, fut).await {
    //         Ok(maybe_message) => {
    //             match maybe_message {
    //                 Ok(message) => Ok(message),
    //                 Err(e) => Err(format!("RPC call error: {}", e).into()),
    //             }
    //         }
    //         Err(e) => {
    //             Err(format!("Read message timeout after: {} sec - {}", timeout.as_secs(), e).into())
    //         }
    //     }
    // }
}
