//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

// use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use crate::frame::Protocol;
use std::time::Duration;
use async_std::{
    channel::{Receiver, Sender},
    io::{stdin, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
    prelude::*,
    task,
    future,
};
use log::{debug, info, warn, error};

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

#[derive(Clone)]
pub struct Client {
    pub(crate) sender: Sender<Frame>,
    pub(crate) receiver: Receiver<Frame>,
    pub protocol: Protocol,
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
                    task::spawn(async move {
                        info!("Starting heart-beat task with period: {}", heartbeat_interval);
                        loop {
                            task::sleep(Duration::from_secs(heartbeat_interval)).await;
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
        self.send_message(&request).await?;
        match future::timeout(Duration::from_millis(DEFAULT_RPC_CALL_TIMEOUT_MS), async move {
            loop {
                let resp = self.receive_message().await?;
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
    pub async fn send_frame(& self, frame: Frame) -> crate::Result<()> {
        self.sender.send(frame).await?;
        Ok(())
    }
    pub async fn receive_frame(&mut self) -> crate::Result<Frame> {
        let frame = self.receiver.recv().await?;
        return Ok(frame)
    }
    pub async fn send_message(& self, msg: &RpcMessage) -> crate::Result<()> {
        let frame = Frame::from_rpcmessage(self.protocol, &msg);
        self.send_frame(frame).await?;
        Ok(())
    }
    pub async fn receive_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.receive_frame().await?;
        let msg = frame.to_rpcmesage()?;
        return Ok(msg)
    }
    pub async fn receive_message_timeout(&mut self, timeout: Duration) -> crate::Result<RpcMessage> {
        future::timeout(timeout, self.receive_message()).await?
    }

    // pub fn as_sender(& self) -> &RpcMessageTx {
    //     &self.send_message_tx
    // }
}

