//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

// use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::stream::Stream;
use tracing::{debug, info, error, instrument};
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use crate::frame::Protocol;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct LoginParams {
    pub user: String,
    pub password: String,
    pub idleWatchDogTimeOut: i32,
    pub protocol: Protocol,
}
impl LoginParams {
    pub fn new(user: &str, password: &str) -> LoginParams {
        LoginParams {
            user: user.into(),
            password: password.into(),
            idleWatchDogTimeOut: 180,
            protocol: Protocol::ChainPack,
        }
    }
    fn to_rpcvalue(&self) -> RpcValue {
        let mut map = chainpack::rpcvalue::Map::new();
        let mut login = chainpack::rpcvalue::Map::new();
        login.insert("user".into(), RpcValue::new(&self.user));
        login.insert("password".into(), RpcValue::new(&self.password));
        login.insert("type".into(), RpcValue::new("PLAIN"));
        map.insert("login".into(), RpcValue::new(login));
        let mut options = chainpack::rpcvalue::Map::new();
        options.insert("idleWatchDogTimeOut".into(), RpcValue::new(self.idleWatchDogTimeOut));
        map.insert("options".into(), RpcValue::new(options));
        RpcValue::new(map)
    }
}

pub struct Client {
    pub connection: Connection,
    send_rpcmessage_tx: mpsc::Sender<RpcMessage>,
    pub send_rpcmessage_rx: mpsc::Receiver<RpcMessage>,
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
    // The `addr` argument is passed directly to `TcpStream::connect`. This
    // performs any asynchronous DNS lookup and attempts to establish the TCP
    // connection. An error at either step returns an error, which is then
    // bubbled up to the caller of `mini_redis` connect.
    let socket = TcpStream::connect(addr).await?;

    // Initialize the connection state. This allocates read/write buffers to
    // perform redis protocol frame parsing.
    let connection = Connection::new(socket);

    let (send_rpcmessage_tx, send_rpcmessage_rx) = mpsc::channel(1);
    Ok(Client {
        connection,
        send_rpcmessage_tx,
        send_rpcmessage_rx,
    })
}

impl Client {

    pub async fn login(&mut self, login_params: &LoginParams) -> crate::Result<()> {
        self.connection.protocol = Some(login_params.protocol);
        let hello = RpcMessage::new_request("hello");
        debug!("login {}", hello);
        self.connection.write_message(&hello).await?;
        let nonce = self.connection.read_message().await?;
        let mut login = RpcMessage::new_request("login");
        login.set_params(login_params.to_rpcvalue());
        self.connection.write_message(&login).await?;
        let login_result = self.connection.read_frame().await?;
        let msg = login_result.to_rpcmesage()?;
        debug!("login result: {}", msg);
        match msg.result() {
            Some(_) => {
                if login_params.idleWatchDogTimeOut > 60 {
                    let timeout = login_params.idleWatchDogTimeOut as u64;
                    let tx = self.send_rpcmessage_tx.clone();
                    let hnd = tokio::spawn(async move {
                        info!("Starting heart-beet task with period: {}", timeout);
                        loop {
                            tokio::time::sleep(Duration::from_secs(timeout)).await;
                            debug!("Heart Beet");
                            let mut msg = RpcMessage::default();
                            msg.set_method("ping");
                            msg.set_shvpath(".broker/app");
                            let res = tx.send(msg).await;
                            match res {
                                Ok(_) => {}
                                Err(e) => error!("cannot send: {}", e),
                            }
                        }
                    });
                }
                Ok(())
            },
            None => Err("Login incorrect!".into())
        }
    }
    //pub async fn read_frame(&mut self) -> crate::Result<Frame> {
    //    self.connection.read_frame().await
    //}
    //pub async fn read_frame_timeout(&mut self, timeout: Duration) -> crate::Result<Frame> {
    //    match tokio::time::timeout(timeout, self.read_frame()).await {
    //        Ok(maybe_frame) => {
    //            maybe_frame
    //        }
    //        Err(e) => {
    //            Err(format!("Read frame timeout after: {} sec - {}", timeout.as_secs(), e).into())
    //        }
    //    }
    //}
}

