//! Minimal shv client implementation

use crate::RpcFrame;

use async_std::{channel::Sender, future, task};
use chainpack::rpcframe::Protocol;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};
use log::{debug, error, info, trace, warn};
use std::time::{Duration, Instant};
use async_std::channel::{bounded, Receiver};
use bytes::BytesMut;
use futures::{AsyncRead, AsyncWrite};
use chainpack::rpcmessage::RqId;

const DEFAULT_RPC_CALL_TIMEOUT_MS: u64 = 5000;

#[derive(Copy, Clone, Debug)]
pub enum LoginType {
    PLAIN,
    SHA1,
}
impl LoginType {
    pub fn to_str(&self) -> &str {
        match self {
            LoginType::PLAIN => "PLAIN",
            LoginType::SHA1 => "SHA1",
        }
    }
}

pub enum Scheme {
    Tcp,
    LocalSocket,
}

#[derive(Clone, Debug)]
pub struct LoginParams {
    pub user: String,
    pub password: String,
    pub login_type: LoginType,
    pub device_id: String,
    pub mount_point: String,
    pub heartbeat_interval: Option<Duration>,
    //pub protocol: Protocol,
}

impl Default for LoginParams {
    fn default() -> Self {
        LoginParams {
            user: "".to_string(),
            password: "".to_string(),
            login_type: LoginType::SHA1,
            device_id: "".to_string(),
            mount_point: "".to_string(),
            heartbeat_interval: Some(Duration::from_secs(60)),
            //protocol: Protocol::ChainPack,
        }
    }
}

impl LoginParams {
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut map = chainpack::rpcvalue::Map::new();
        let mut login = chainpack::rpcvalue::Map::new();
        login.insert("user".into(), RpcValue::from(&self.user));
        login.insert("password".into(), RpcValue::from(&self.password));
        login.insert("type".into(), RpcValue::from(self.login_type.to_str()));
        map.insert("login".into(), RpcValue::from(login));
        let mut options = chainpack::rpcvalue::Map::new();
        if let Some(hbi) = self.heartbeat_interval {
            options.insert(
                "idleWatchDogTimeOut".into(),
                RpcValue::from(hbi.as_secs() * 3),
            );
        }
        let mut device = chainpack::rpcvalue::Map::new();
        if !self.device_id.is_empty() {
            device.insert("deviceId".into(), RpcValue::from(&self.device_id));
        } else if !self.mount_point.is_empty() {
            device.insert("mountPoint".into(), RpcValue::from(&self.mount_point));
        }
        if !device.is_empty() {
            options.insert("device".into(), RpcValue::from(device));
        }
        map.insert("options".into(), RpcValue::from(options));
        RpcValue::from(map)
    }
}
/*
#[derive(Clone, Debug)]
pub struct ConnectionParams {
    pub url: Url,
    pub login_params: LoginParams,
}
impl ConnectionParams {
    pub fn new(url: Url, login_params: LoginParams) -> ConnectionParams {
        ConnectionParams {
            url,
            login_params,
        }
    }
}
*/

pub trait AsyncRW: AsyncRead + AsyncWrite {}
impl<T> AsyncRW for T where T: AsyncRead + AsyncWrite {}

pub type AsyncRWBox = Box<dyn AsyncRW + Unpin + Send>;
//pub type ClientTx = Sender<RpcFrame>;
//pub type ClientRx = async_broadcast::Receiver<RpcFrame>;

// pub type FrameSender = Sender<RpcFrame>;
// pub type FrameReceiver = Receiver<RpcFrame>;
// pub type MessageSender = Sender<RpcMessage>;
// pub type ToHandlerMessageSender = Sender<(RpcMessage, i32)>;
// pub type FromSlotMessageReceiver = Receiver<(RpcMessage, i32)>;
// pub type MessageReceiver = Receiver<RpcMessage>;

//#[derive(Clone)]
pub struct RpcSlot {
    pub handler_id: i32,
    pub sender: ToHandlerMessageSender,
    pub receiver: MessageReceiver,
}

impl RpcSlot {
    pub async fn send_message(&self, msg: RpcMessage) -> crate::Result<()> {
        self.sender.send((msg, self.handler_id)).await?;
        Ok(())
    }
    pub async fn receive_message(& self) -> crate::Result<RpcMessage> {
        let msg = self.receiver.recv().await?;
        return Ok(msg);
    }
}

pub struct RpcSlotHandler {
    pub request_id: Option<RqId>,
    pub sender: MessageSender,
}

pub struct Client {
    stream: AsyncRWBox,
    // The buffer for reading frames.
    buffer: BytesMut,

    slot_receiver: Receiver<(RpcMessage, i32)>,
    slot_sender: Sender<(RpcMessage, i32)>,

    slot_handlers: Vec<RpcSlotHandler>,

    pub protocol: Protocol,
}

// pub enum SlotType {
//     Request,
//     Notify,
// }

impl Client {
    pub fn create_slot(&mut self, slot_type: SlotType) -> RpcSlot {
        let handler_id = self.handlers.len() as i32;
        let request_id = match slot_type {
            SlotType::Request => None,
            SlotType::Notify => Some(0),
        };
        let (s, r) = bounded(1);
        self.handlers.push(RpcSlotHandler {
            request_id,
            sender: s,
        }
        );
        RpcSlot {
            handler_id,
            sender: self.slot_sender.clone(),
            receiver: r,
        }
    }
    pub async fn login(&mut self, login_params: &LoginParams) -> crate::Result<()> {
        let hello_resp = self
            .call_rpc_method("", "hello", None)
            .await?;
        debug!("hello resp {}", hello_resp);
        let mut login_params = login_params.clone();
        if let LoginType::SHA1 = login_params.login_type {
            if let Some(result) = hello_resp.result() {
                if let Some(nonce) = result.as_map().get("nonce") {
                    let hash = crate::utils::sha1_password_hash(
                        login_params.password.as_bytes(),
                        nonce.as_str().as_bytes(),
                    );
                    login_params.password = hash;
                } else {
                    warn!("nonce param missing!");
                }
            } else {
                warn!("hello response params missing!");
            }
        }
        let login_resp = self.call_rpc_method("", "login", Some(login_params.to_rpcvalue())).await?;
        debug!("login result: {}", login_resp);
        Ok(())
    }

    pub async fn call_rpc_method(&mut self, shvpath: &str, method: &str, params: Option<RpcValue>) -> crate::Result<RpcMessage> {
        let rq = RpcMessage::create_request(shvpath, method, params);
        let rq_id = rq.request_id().ok_or("Request ID missing")?;
        trace!("sending RPC request id: {} msg: {}", rq_id, rq);
        let slot = self.create_slot(SlotType::Request);
        slot.send_message(rq).await?;
        let resp = slot.receive_message().await?;
        Ok(resp)
    }
}


