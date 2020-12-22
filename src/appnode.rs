use chainpack::{RpcMessage, RpcValue, RpcMessageMetaTags, metamethod};
use tracing::debug;
use chainpack::metamethod::{MetaMethod, Signature};
use crate::shvnode::ShvNode;
use async_trait::async_trait;

pub struct BasicNode {
    pub methods: Vec<MetaMethod>,
}
impl BasicNode {
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
impl<'a> ShvNode<'a> for BasicNode {
    fn methods(&'a self, path: &[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        if path.is_empty() {
            return Ok(self.methods.iter().map(|mm: &MetaMethod| {mm}).collect())
        }
        Err(format!("BasicNode::methods() - Invalid path: {:?}", path).into())
    }
    fn children(&'a self, path: &[&str]) -> crate::Result<Vec<(&'a str, Option<bool>)>> {
        if path.is_empty() {
            return Ok(Vec::new())
        }
        Err(format!("BasicNode::children() - Invalid path: {:?}", path).into())
    }
    async fn call_method(&'a self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        if path.is_empty() {
            if method == "dir" {
                let mut method_pattern = "".to_string();
                let mut attrs_pattern = 0;
                if let Some(params) = params {
                    if params.is_list() {
                        let params = params.as_list();
                        if params.len() >= 1 {
                            method_pattern = params[0].as_str()?.to_string();
                        }
                        if params.len() >= 2 {
                            //debug!("param [1]: {}", params[1]);
                            attrs_pattern = params[1].as_u32();
                        }
                    } else {
                        method_pattern = params.to_string();
                    }
                }
                debug!("method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
                return self.dir(path, &method_pattern, attrs_pattern);
            }
            if method == "ls" {
                let mut name_pattern = "".to_string();
                let mut with_children_info = false;
                if let Some(params) = params {
                    if params.is_list() {
                        let params = params.as_list();
                        if params.len() >= 1 {
                            name_pattern = params[0].as_str()?.to_string();
                        }
                        if params.len() >= 2 {
                            //debug!("param [1]: {}", params[1]);
                            with_children_info = (params[1].as_u32() != 0);
                        }
                    } else {
                        name_pattern = params.to_string();
                    }
                }
                debug!("name pattern: {}, with_children_info: {}", name_pattern, with_children_info);
                return self.ls(path, &name_pattern, with_children_info);
            }
        }
        Err(format!("Invalid method: '{}' on path: '{:?}' called", method, path).into())
        //Err(format!("BasicNode::call_method() - Invalid path: {:?}", path).into())
    }
}
pub struct AppNode {
    pub app_name: String,
    pub device_id: String,
    pub device_type: String,
    pub methods: Vec<MetaMethod>,
    super_node: BasicNode,
}

impl AppNode {
    pub fn new() -> Self {
        Self {
            app_name: "".into(),
            device_id: "".into(),
            device_type: "".into(),
            methods: vec![
                // MetaMethod { name: "dir".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                // MetaMethod { name: "ls".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "appName".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "deviceId".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                MetaMethod { name: "deviceType".into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            ],
            super_node: BasicNode::new(),
        }
    }
}
#[async_trait]
impl<'a> ShvNode<'a> for AppNode {
    fn methods(&'a self, path: &[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        if path.is_empty() {
            let mut lst1 = self.super_node.methods(path)?;
            let mut lst2 = self.methods.iter().map(|mm: &MetaMethod| { mm }).collect();
            lst1.append(&mut lst2);
            return Ok(lst1)
        }
        Err(format!("AppNode::methods() - Invalid path: {:?}", path).into())
    }

    fn children(&'a self, path: &[&str]) -> crate::Result<Vec<(&'a str, Option<bool>)>> {
        Ok(Vec::new())
    }

    async fn call_method(&'a self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
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
        self.super_node.call_method(path, method, params).await
    }
}