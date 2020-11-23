//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

// use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection};

use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, info, warn, error};
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use crate::frame::Protocol;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use chainpack::rpcmessage::RqId;
use tokio::sync::watch::Receiver;
use sha1::{Sha1, Digest};

const DEFAULT_RPC_CALL_TIMEOUT_MS: u64 = 5000;

#[derive(Copy, Clone)]
pub enum PasswordType {
    PLAIN,
    SHA1
}
impl PasswordType {
    pub fn to_str(&self) -> &str {
        match self {
            PasswordType::PLAIN => "PLAIN",
            PasswordType::SHA1 => "SHA1",
        }
    }
 }

#[derive(Clone)]
pub struct ConnectionParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub password_type: PasswordType,
    pub idle_watchdog_timeout: i32,
    pub protocol: Protocol,
}
impl ConnectionParams {
    pub fn new(host: &str, port: u16, user: &str, password: &str) -> ConnectionParams {
        ConnectionParams {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            password_type: if user.len() == 40 { PasswordType::SHA1} else { PasswordType::PLAIN },
            idle_watchdog_timeout: 180,
            protocol: Protocol::ChainPack,
        }
    }
    fn to_rpcvalue(&self) -> RpcValue {
        let mut map = chainpack::rpcvalue::Map::new();
        let mut login = chainpack::rpcvalue::Map::new();
        login.insert("user".into(), RpcValue::new(&self.user));
        login.insert("password".into(), RpcValue::new(&self.password));
        login.insert("type".into(), RpcValue::new(self.password_type.to_str()));
        map.insert("login".into(), RpcValue::new(login));
        let mut options = chainpack::rpcvalue::Map::new();
        options.insert("idleWatchDogTimeOut".into(), RpcValue::new(self.idle_watchdog_timeout));
        map.insert("options".into(), RpcValue::new(options));
        RpcValue::new(map)
    }
}

pub struct Client {
    pub connection: Connection,

    pub send_rpcmessage_tx: mpsc::Sender<RpcMessage>,
    send_rpcmessage_rx: mpsc::Receiver<RpcMessage>,

    watch_rpcmessage_tx: watch::Sender<RpcMessage>,
    pub watch_rpcmessage_rx: watch::Receiver<RpcMessage>,
}

pub async fn connect(params: &ConnectionParams) -> crate::Result<Client> {
    // The `addr` argument is passed directly to `TcpStream::connect`. This
    // performs any asynchronous DNS lookup and attempts to establish the TCP
    // connection. An error at either step returns an error, which is then
    // bubbled up to the caller of `mini_redis` connect.
    let addr = format!("{}:{}", params.host, params.port);
    info!("connecting to: {}", addr);
    let socket = TcpStream::connect(addr.clone()).await?;

    info!("connected to: {}", addr);
    // Initialize the connection state. This allocates read/write buffers to
    // perform redis protocol frame parsing.
    let connection = Connection::new(socket);

    let (send_rpcmessage_tx, send_rpcmessage_rx) = mpsc::channel(1);
    let (rec_rpcmessage_tx, rec_rpcmessage_rx) = watch::channel(RpcMessage::default());
    Ok(Client {
        connection,
        send_rpcmessage_tx,
        send_rpcmessage_rx,
        watch_rpcmessage_tx: rec_rpcmessage_tx,
        watch_rpcmessage_rx: rec_rpcmessage_rx,
    })
}

impl Client {

    pub async fn login(&mut self, login_params: &ConnectionParams) -> crate::Result<()> {
        self.connection.protocol = Some(login_params.protocol);
        let hello = RpcMessage::new_request("", "hello", None);
        self.connection.send_message(&hello).await?;
        let hello_resp = self.connection.recv_message().await?;
        debug!("hello resp {}", hello_resp);
        let mut login_params = login_params.clone();
        if login_params.password.len() != 40 {
            if let Some(result) = hello_resp.result() {
                if let Some(nonce) = result.as_map().get("nonce") {
                    let mut hasher = Sha1::new();
                    hasher.update(login_params.password.as_bytes());
                    let result = hasher.finalize();
                    let hash = hex::encode(&result[..]);
                    let hash = format!("{}{}", nonce.as_str(), hash);

                    //debug!("login password hash1: {}", hash);
                    let mut hasher = Sha1::new();
                    hasher.update(hash.as_bytes());
                    let result = hasher.finalize();
                    let hash = hex::encode(&result[..]);
                    //debug!("login password hash2: {}", hash);
                    login_params.password = hash;
                    login_params.password_type = PasswordType::SHA1;
                } else {
                    warn!("nonce param missing!");
                }
            } else {
                warn!("hello response params missing!");
            }
        }
        let login = RpcMessage::new_request("", "login", Some(login_params.to_rpcvalue()));
        self.connection.send_message(&login).await?;
        let login_result = self.connection.recv_frame().await?;
        let msg = login_result.to_rpcmesage()?;
        debug!("login result: {}", msg);
        match msg.result() {
            Some(_) => {
                let heartbeat_interval = login_params.idle_watchdog_timeout as u64 / 3;
                if heartbeat_interval >= 60 {
                    let tx = self.send_rpcmessage_tx.clone();
                    tokio::spawn(async move {
                        info!("Starting heart-beat task with period: {}", heartbeat_interval);
                        loop {
                            tokio::time::sleep(Duration::from_secs(heartbeat_interval)).await;
                            debug!("Heart Beat");
                            let mut msg = RpcMessage::default();
                            msg.set_request_id(RpcMessage::next_request_id());
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
    pub async fn message_loop(&mut self) -> crate::Result<()> {
        loop {
            debug!("entering select");
            tokio::select! {
                maybe_msg = self.connection.recv_message() => {
                    debug!(?maybe_msg);
                    match maybe_msg {
                        Ok(msg) => {
                            info!("message received: {}", msg);
                            if let Err(e) = self.watch_rpcmessage_tx.send(msg) {
                                warn!("Send rpc message error: {}", e);
                            }
                        }
                        Err(e) => {
                            return Err(e.into())
                        }
                    }
                }
                maybe_signal = self.send_rpcmessage_rx.recv() => {
                    match maybe_signal {
                        Some(msg) => {
                            info!("send signal request: {}", msg);
                            if let Err(e) = self.connection.send_message(&msg).await {
                                warn!("send signal error: {}", e);
                            }
                        }
                        None => {
                            warn!("send EMPTY signal request");
                        }
                    }
                }
            }

        }
    }
    pub async fn wait_for_response(rx: watch::Receiver<RpcMessage>, rq_id: RqId) -> crate::Result<RpcMessage> {
        Self::wait_for_response_timeout(rx, rq_id, Duration::from_millis(DEFAULT_RPC_CALL_TIMEOUT_MS)).await
    }
    pub async fn wait_for_response_timeout(rx: watch::Receiver<RpcMessage>, rq_id: RqId, timeout: Duration) -> crate::Result<RpcMessage> {
        let fut = async move {
            let mut rx = rx;
            loop {
                match rx.changed().await {
                    Ok(_) => {
                        let resp = rx.borrow();
                        if let Some(id) = resp.request_id() {
                            if id == rq_id {
                                return Ok(resp.clone())
                            }
                        }
                    }
                    Err(e) => {
                        return Err(format!("Read channel error {}", e))
                    }
                }
            }
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(message) => {
                match message {
                    Ok(message) => Ok(message),
                    Err(e) => Err(e.into())
                }
            }
            Err(e) => {
                Err(format!("Read message timeout after: {} sec - {}", timeout.as_secs(), e).into())
            }
        }
    }

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

