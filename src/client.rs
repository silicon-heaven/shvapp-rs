//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

// use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::stream::Stream;
use tracing::{debug, instrument};
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use crate::frame::Protocol;

pub struct LoginParams {
    pub user: String,
    pub password: String,
    pub idleWatchDogTimeOut: i32,
}
impl LoginParams {
    pub fn new(user: &str, password: &str) -> LoginParams {
        LoginParams {
            user: user.into(),
            password: password.into(),
            idleWatchDogTimeOut: 180,
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
    connection: Connection,
}

/// A message received on a subscribed channel.
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
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

    Ok(Client { connection })
}

impl Client {

    pub async fn login(&mut self, login_params: &LoginParams) -> crate::Result<()> {
        let hello = RpcMessage::new_request("hello");
        let hello = Frame::from_rpcmessage(Protocol::ChainPack, &hello);
        debug!("login {}", hello);
        self.connection.write_frame(&hello).await?;
        let nonce = self.connection.read_frame().await?;
        let mut login = RpcMessage::new_request("login");
        login.set_params(login_params.to_rpcvalue());
        let login = Frame::from_rpcmessage(Protocol::ChainPack, &login);
        self.connection.write_frame(&login).await?;
        let login_result = self.connection.read_frame().await?;
        let msg = login_result.to_rpcmesage()?;
        debug!("login result: {}", msg);
        match msg.result() {
            Some(_) => Ok(()),
            None => Err("Login incorrect!".into())
        }
    }
    /// Reads a response frame from the socket.
    ///
    /// If an `Error` frame is received, it is converted to `Err`.
    pub async fn read_frame(&mut self) -> crate::Result<Frame> {
        self.connection.read_frame().await
    }
    pub async fn read_frame_timeout(&mut self, timeout: Duration) -> crate::Result<Frame> {
        match tokio::time::timeout(timeout, self.read_frame()).await {
            Ok(maybe_frame) => {
                maybe_frame
            }
            Err(e) => {
                Err(format!("Read frame timeout after: {} sec - {}", timeout.as_secs(), e).into())
            }
        }
    }
}

