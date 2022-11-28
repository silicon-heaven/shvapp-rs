use chainpack::rpcframe::{RpcFrame, Protocol};
use crate::client::{Client};
use bytes::{Buf, BytesMut};
use chainpack::{ChainPackWriter, Writer, CponWriter};
use log::{debug, error};
use async_std::{
    channel::{Receiver},
    // io::{stdin, BufReader, BufWriter},
    prelude::*,
    // task,
};
use futures::{select, FutureExt, AsyncWrite, AsyncRead};
use async_broadcast::{broadcast};

enum LogFramePrompt {
    Send,
    Receive,
}

pub trait AsyncRW: AsyncRead + AsyncWrite {}
impl<T> AsyncRW for T where T: AsyncRead + AsyncWrite {}

pub type AsyncRWBox = Box<dyn AsyncRW + Unpin + Send>;

//#[derive(Debug)]
pub struct Connection {
    stream: AsyncRWBox,
    // The buffer for reading frames.
    buffer: BytesMut,
    from_client: Receiver<RpcFrame>,
    // to_client: (Sender<Frame>, Receiver<Frame>),
    // we cannot use async-std MPMC channel because only one receiver is notified here
    // and we need to broadcast message to all the clients
    // https://docs.rs/async-broadcast/latest/async_broadcast/#difference-with-async-channel
    to_client: async_broadcast::Sender<RpcFrame>,
}

impl Connection {
    pub fn new(stream: AsyncRWBox, protocol: Protocol) -> (Connection, Client) {
        // to_client_capacity 1 should be sufficient, the socket reader will be blocked
        // if the channel will be full (in case of async_broadcast implementation).
        // If some client will not read receive end, then whole app will be blocked !!!
        // BUT the login function makes 2 RPC calls on self clones (because of RPC timeout, see Client::call_rpc_method),
        // this is why capacity must be >= 2
        const TO_CLIENT_CHANNEL_CAPACITY: usize = 2;
        const FROM_CLIENT_CHANNEL_CAPACITY: usize = 256;
        let (from_client_sender, from_client_receiver) = async_std::channel::bounded(FROM_CLIENT_CHANNEL_CAPACITY);
        let (mut to_client_sender, to_client_receiver) = broadcast(TO_CLIENT_CHANNEL_CAPACITY);
        to_client_sender.set_overflow(true);
        (
            Connection {
                stream,
                buffer: BytesMut::with_capacity(4 * 1024),
                from_client: from_client_receiver,
                to_client: to_client_sender,
                // unfortunately, async-std channel is dispatch not broadcast
                // we have to use broadcast channel from async-broadcast crate
            },
            Client {
                sender: from_client_sender,
                receiver: to_client_receiver,
                protocol
            }
        )
    }
    pub async fn exec(&mut self) -> crate::Result<()> {
        let mut frame_cnt = 1;
        loop {
            let mut buf: [u8; 1024] = [0; 1024];
            select! {
                n = self.stream.read(&mut buf).fuse() => match n {
                    Ok(n) => {
                        debug!("{} bytes read from socket", n);
                        if n == 0 {
                            return Err("socket closed by peer.".into())
                        }
                        self.buffer.extend_from_slice(&buf[..n]);
                        loop {
                            match self.receive_frame() {
                                Ok(frame) => match frame {
                                    Some(frame) => {
                                        debug!("{} sender is full: {}, Sending frame to clients ............: {}", frame_cnt, self.to_client.is_full(), &frame);
                                        //if self.to_client.is_full() {
                                        //    error!("sender is full, capacity: {}", self.to_client.capacity());
                                        //}
                                        self.to_client.broadcast(frame).await?;
                                        debug!("{} ............ SENT", frame_cnt);
                                        frame_cnt += 1;
                                    }
                                    None => {
                                        // not all frame data received
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("read frame error {}", e);
                                    break;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        error!("read socket error {}", e);
                    },
                },
                frame = self.from_client.recv().fuse() => match frame {
                    Ok(frame) => {
                        debug!("Frame to send from client: {}", &frame);
                        self.send_frame(&frame).await?;
                    }
                    Err(e) => {
                        error!("read frame error {}", e);
                    },
                }
            }
        }
    }
    fn receive_frame(&mut self) -> crate::Result<Option<RpcFrame>> {
        let buff = &self.buffer[..];
        match RpcFrame::parse(buff) {
            Ok(maybe_frame) => {
                match maybe_frame {
                    None => { return Ok(None); }
                    Some((frame_len, frame)) => {
                        self.buffer.advance(frame_len);
                        Connection::log_frame(&frame, LogFramePrompt::Receive);
                        Ok(Some(frame))
                    }
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn send_frame(&mut self, frame: &RpcFrame) -> crate::Result<()> {
        Connection::log_frame(&frame, LogFramePrompt::Send);
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

    fn log_frame(frame: &RpcFrame, prompt: LogFramePrompt) {
        let prompt_str = match prompt { LogFramePrompt::Send => "<===", LogFramePrompt::Receive => "===>" };
        if frame.data.len() < 1024 {
            match frame.to_rpcmesage() {
                Ok(rpcmessage) => {
                    debug!(target: "rpcmsg", "{} {}", prompt_str, rpcmessage);
                }
                Err(_) => {
                    debug!(target: "rpcmsg", "{} {}", prompt_str, frame);
                }
            }
        } else {
            debug!(target: "rpcmsg", "{} {}", prompt_str, frame);
        }
    }
}
