use chainpack::{RpcValue, metamethod};
use tracing::debug;
use chainpack::metamethod::{MetaMethod, Signature};
use crate::shvnode::{ShvNode};
use async_trait::async_trait;

pub struct ShvTreeNode {
    pub methods: Vec<MetaMethod>,
}
impl ShvTreeNode {
    pub fn new() -> Self {
        Self {
            methods: vec![
                MetaMethod { name: "dir".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "ls".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            ]
        }
    }
}

#[async_trait]
impl ShvNode for ShvTreeNode {
    fn dir<'a>(&'a self, path: &'_[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        if path.is_empty() {
            return Ok(self.methods.iter().map(|mm: &MetaMethod| {mm}).collect())
        }
        return Ok(Vec::new())
    }

    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        unimplemented!()
    }
}

pub struct ApplicationMethods {
    pub app_name: String,
    pub device_id: String,
    pub device_type: String,
    methods: Vec<MetaMethod>,
}

impl ApplicationMethods {
    pub fn new(app_name: &str, device_type: &str, device_id: &str) -> Self {
        Self {
            app_name: app_name.to_string(),
            device_type: device_type.to_string(),
            device_id: device_id.to_string(),
            methods: vec![
                // MetaMethod { name: "dir".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                // MetaMethod { name: "ls".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "appName".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "deviceId".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "deviceType".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            ],
        }
    }
}
#[async_trait]
impl ShvNode for ApplicationMethods {
    fn dir<'a>(&'a self, path: &'_[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        if path.is_empty() {
            return Ok(self.methods.iter().map(|mm: &MetaMethod| {mm}).collect())
        }
        return Ok(Vec::new())
    }

    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        if path.is_empty() {
            if method == "appName" {
                return Ok(RpcValue::new(&self.app_name))
            }
            if method == "deviceId" {
                return Ok(RpcValue::new(&self.device_id))
            }
            if method == "deviceType" {
                return Ok(RpcValue::new(&self.device_type))
            }
        }
        Err(format!("Unknown method: {} on path: {:?}", method, path).into())
    }
}