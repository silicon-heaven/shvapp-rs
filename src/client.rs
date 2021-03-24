//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

// use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection};

use tokio::net::{TcpStream};
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use crate::frame::Protocol;
use std::time::Duration;
use tokio::sync::{mpsc, broadcast};
use log::{debug, info, warn, error};

const DEFAULT_RPC_CALL_TIMEOUT_MS: u64 = 5000;

pub type RpcMessageTx = mpsc::Sender<RpcMessage>;

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
    pub device_id: String,
    pub mount_point: String,
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
            device_id: "".into(),
            mount_point: "".into(),
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
        let mut device = chainpack::rpcvalue::Map::new();
        if !self.device_id.is_empty() {
            device.insert("deviceId".into(), RpcValue::new(&self.device_id));
        }
        else if !self.mount_point.is_empty() {
            device.insert("mountPoint".into(), RpcValue::new(&self.mount_point));
        }
        if !device.is_empty() {
            options.insert("device".into(), RpcValue::new(device));
        }
        map.insert("options".into(), RpcValue::new(options));
        RpcValue::new(map)
    }
}

pub struct Client {
    //connection: Arc<ClientConnection>,
    send_message_tx: RpcMessageTx,
    recv_message_rx: broadcast::Receiver<RpcMessage>,
    // recv_message_rx cannot be cloned, we have to keep TX end to make TX.subscribe() happen
    // when cloning client
    dummy_recv_response_tx: broadcast::Sender<RpcMessage>,
    // when broadcast sender is dropped, its receiver receives empty message
    // mpsc has not this function AFIK
    // broadcast channel is only one, I know, which will send something on TX end drop
    #[allow(dead_code)]
    client_dropped_tx: broadcast::Sender<()>,
}
impl Clone for Client {
    fn clone(&self) -> Self {
        Client {
            send_message_tx: self.send_message_tx.clone(),
            recv_message_rx: self.dummy_recv_response_tx.subscribe(),
            dummy_recv_response_tx: self.dummy_recv_response_tx.clone(),
            client_dropped_tx: self.client_dropped_tx.clone(),
        }
    }
}

pub async fn connect(params: &ConnectionParams) -> crate::Result<(Client, ClientConnection)> {
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
    let mut connection = Connection::new(socket);
    connection.protocol = Some(params.protocol);

    const BUFF_LEN: usize = 1024;
    let (send_message_tx, send_request_rx) = mpsc::channel(BUFF_LEN);
    let (recv_response_tx, recv_message_rx) = broadcast::channel(BUFF_LEN);
    let (client_dropped_tx, client_dropped_rx) = broadcast::channel(1);
    Ok((
        Client {
            send_message_tx,
            recv_message_rx,
            dummy_recv_response_tx: recv_response_tx.clone(),
            client_dropped_tx,
        },
        ClientConnection {
            connection,
            send_request_rx,
            recv_response_tx,
            client_dropped_rx,
        },
    ))
}


pub struct ClientConnection {
    connection: Connection,
    send_request_rx: mpsc::Receiver<RpcMessage>,
    recv_response_tx: broadcast::Sender<RpcMessage>,
    client_dropped_rx: broadcast::Receiver<()>,
}
impl ClientConnection {
    pub async fn exec(&mut self) -> crate::Result<()> {
        loop {
            tokio::select! {
                resp = self.connection.recv_message() => {
                    match resp {
                        Ok(resp) => {
                            // debug!(?maybe_resp);
                            debug!(target: "rpcmsg", "R==> message received: {}", resp);
                            self.recv_response_tx.send(resp)?;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                rq = self.send_request_rx.recv() => {
                    match rq {
                        Some(rq) => {
                            debug!(target: "rpcmsg", "<==S send request: {}", rq);
                            self.connection.send_message(&rq).await?;
                        }
                        None => {
                            info!("Ignoring empty request, client disconnected?");
                        }
                    }
                }
                _  = self.client_dropped_rx.recv() => {
                    // all the client clones have dropped their client_dropped_tx end
                    info!("finished received");
                    return Ok(())
                }
            }
        }
    }
}
impl Drop for ClientConnection {
    fn drop(&mut self) {
        warn!("Dropping ClientConnection");
    }
}

impl Client {

    pub async fn login(&mut self, login_params: &ConnectionParams) -> crate::Result<()> {
        let hello_resp = self.call(RpcMessage::create_request("", "hello", None)).await?;
        debug!("hello resp {}", hello_resp);
        let mut login_params = login_params.clone();
        if login_params.password.len() != 40 {
            if let Some(result) = hello_resp.result() {
                if let Some(nonce) = result.as_map().get("nonce") {
                    let hash = crate::utils::sha1_password_hash(login_params.password.as_str().as_bytes(), nonce.as_str().as_bytes());
                    login_params.password = hash;
                    login_params.password_type = PasswordType::SHA1;
                } else {
                    warn!("nonce param missing!");
                }
            } else {
                warn!("hello response params missing!");
            }
        }
        let login_resp = self.call(RpcMessage::create_request("", "login", Some(login_params.to_rpcvalue()))).await?;
        debug!("login result: {}", login_resp);
        match login_resp.result() {
            Some(_) => {
                let heartbeat_interval = login_params.idle_watchdog_timeout as u64 / 3;
                if heartbeat_interval >= 60 {
                    let mut client = self.clone();
                    tokio::spawn(async move {
                        info!("Starting heart-beat task with period: {}", heartbeat_interval);
                        loop {
                            tokio::time::sleep(Duration::from_secs(heartbeat_interval)).await;
                            debug!("Sending heart beat");
                            let res = client.call(RpcMessage::create_request(".broker/app", "ping", None)).await;
                            match res {
                                Ok(_) => {}
                                Err(e) => error!("cannot send ping: {}", e),
                            }
                        }
                    });
                }
                Ok(())
            },
            None => Err("Login incorrect!".into())
        }
    }

    pub async fn call(&mut self, request: RpcMessage) -> crate::Result<RpcMessage> {
        if !request.is_request() {
            return Err("Not request".into())
        }
        let rq_id = request.request_id().ok_or("Request ID missing")?;
        debug!("sending RPC request id: {} .............. {}", rq_id, request);
        self.send_message_tx.send(request).await?;
        match tokio::time::timeout(Duration::from_millis(DEFAULT_RPC_CALL_TIMEOUT_MS), async move {
            loop {
                let resp = self.recv_message_rx.recv().await?;
                if let Some(id) = resp.request_id() {
                    if id == rq_id {
                        let resp = resp.clone();
                        debug!("{} .............. got response: {}", rq_id, resp);
                        return Ok(resp)
                    }
                }
            }
        }).await {
            Ok(resp) => resp,
            Err(_) => Err(format!("Response to request id: {} didn't arrive within {} msec.", rq_id, DEFAULT_RPC_CALL_TIMEOUT_MS).into()),
        }
    }
    pub async fn send(& self, msg: RpcMessage) -> crate::Result<()> {
        self.send_message_tx.send(msg).await?;
        Ok(())
    }
    pub async fn receive(&mut self) -> crate::Result<RpcMessage> {
        let msg = self.recv_message_rx.recv().await?;
        return Ok(msg)
    }
    pub async fn receive_timeout(&mut self, timeout: Duration) -> crate::Result<RpcMessage> {
        tokio::time::timeout(timeout, self.receive()).await?
    }

    pub fn as_sender(& self) -> &RpcMessageTx {
        &self.send_message_tx
    }
}

