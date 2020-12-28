use chainpack::metamethod::{MetaMethod, LsAttribute};
use chainpack::rpcvalue::List;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags};
use tracing::debug;
use async_trait::async_trait;
use crate::utils;
use std::future::Future;
use std::pin::Pin;

// #[derive(Debug)]
// pub struct ShvError {
//     pub message: String
// }
//
// impl ShvError {
//     fn new(msg: &str) -> ShvError {
//         ShvError{ message: msg.to_string()}
//     }
// }
//
// impl fmt::Display for ShvError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f,"{}",self.message)
//     }
// }
//
// impl Error for ShvError {
//     fn description(&self) -> &str {
//         &self.message
//     }
// }

pub struct ShvNode {
    pub name: String,
    pub processors: Vec<Box<dyn RpcProcessor>>,
    pub child_nodes: Vec<Box<ShvNode>>,
}

impl ShvNode {
    fn add_child_node(&mut self, nd: Box<ShvNode>) -> &mut Self {
        self.child_nodes.push(nd);
        self
    }
    fn has_child_nodes(&self) -> Option<bool> {
        Some(!self.child_nodes.is_empty())
    }

    pub fn process_request(&mut self, request1: &RpcMessage) -> Pin<Box<dyn Future<Output = crate::Result<RpcValue>> + '_>>  {
        let request = request1.clone();
        Box::pin(async move {
            if !request.is_request() {
                return Err("Not request".into());
            }
            debug!("request: {}", request);
            let shv_path = request.shv_path().unwrap_or("");
            let path = utils::split_shv_path(shv_path);
            debug!("path: {:?}", path);
            let method = request.method().ok_or("Method is empty")?;
            debug!("method: {}", method);
            let params = request.params();
            if !path.is_empty() {
                let node_name = path[0];
                for ch in self.child_nodes.iter_mut() {
                    if node_name == ch.name {
                        let mut rq2 = request.clone();
                        rq2.set_shvpath(&utils::join_shv_path(&path[1..]));
                        return ch.process_request(&rq2).await;
                    }
                }
            }
            for p in self.processors.iter_mut() {
                for m in p.methods(&path) {
                    // TDDO: check access rights
                    if m.name == method {
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
                            debug!("dir - method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
                            return Ok(self.dir(&path, &method_pattern, attrs_pattern));
                        } else if m.name == "ls" {
                            let mut name_pattern = "".to_string();
                            let mut ls_attrs = 0;
                            if let Some(params) = params {
                                if params.is_list() {
                                    let params = params.as_list();
                                    if params.len() >= 1 {
                                        name_pattern = params[0].as_str()?.to_string();
                                    }
                                    if params.len() >= 2 {
                                        //debug!("param [1]: {}", params[1]);
                                        ls_attrs = params[1].as_u32();
                                    }
                                } else {
                                    name_pattern = params.to_string();
                                }
                            }
                            debug!("name pattern: {}, with_children_info: {}", name_pattern, ls_attrs);
                            return Ok(self.ls(&path, &name_pattern, ls_attrs));
                        } else {
                            return p.call_method(&path, method, params).await
                        }
                    }
                }
            }
            Err(format!("Unknown method: {} on path: {:?}", method, path).into())
        })
    }
    fn dir(& self, path: &[&str], method_pattern: &str, attrs_pattern: u32) -> RpcValue {
        debug!("dir method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
        let mut lst: List = Vec::new();
        for p in self.processors.iter() {
            for mm in p.methods(path) {
                if method_pattern.is_empty() {
                    lst.push(mm.dir_attributes(attrs_pattern as u8));
                }
                else if method_pattern == mm.name {
                    lst.push(mm.dir_attributes(attrs_pattern as u8));
                    break;
                }
            }
        }
        debug!("dir: {:?}", lst);
        return RpcValue::new(lst);
    }
    fn ls(& self, path: &[&str], name_pattern: &str, ls_attrs_pattern: u32) -> RpcValue {
        let with_children_info = (ls_attrs_pattern & (LsAttribute::HasChildren as u32)) != 0;
        debug!("ls name_pattern: {}, with_children_info: {}", name_pattern, with_children_info);
        let filter = |it: &'_ &(String, Option<bool>)| {
            if !name_pattern.is_empty() {
                name_pattern == it.0
            } else {
                true
            }
        };
        let map = |it: & (String, Option<bool>)| -> RpcValue {
            if with_children_info {
                let mut lst = List::new();
                lst.push(RpcValue::new(&it.0));
                match it.1 {
                    Some(b) => lst.push(RpcValue::new(b)),
                    None => lst.push(RpcValue::new(())),
                }
                RpcValue::new(lst)
            } else {
                RpcValue::new(&it.0)
            }
        };
        let mut lst = List::new();
        for p in self.processors.iter() {
            lst.extend(p.children(path).iter().filter(filter).map(map));
        }
        for nd in self.child_nodes.iter() {
            //let a = (nd.name.clone(), nd.has_child_nodes());
            let a = (nd.name.clone(), Some(true));
            if filter(&&a) {
                lst.push(map(&a));
            }
        }

           // .filter(|it|{true}).map(map)
        // debug!("dir: {:?}", lst);
        return RpcValue::new(lst);
    }
}

#[async_trait]
pub trait RpcProcessor: Send + Sync {
    fn methods(&self, path: &[&str]) -> Vec<&'_ MetaMethod>;
    fn children(&self, path: &[&str]) -> Vec<(String, Option<bool>)>;
    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue>;
}