use chainpack::metamethod::{MetaMethod, LsAttribute};
use chainpack::rpcvalue::List;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags, metamethod};
use async_trait::async_trait;
use crate::utils;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::client::RpcMessageSender;
use tracing::{warn, info, debug};

pub type ProcessRequestResult = crate::Result<Option<RpcValue>>;
pub struct NodesTree {
    pub root: TreeNode,
}
impl NodesTree {
    pub fn new(root: TreeNode) -> Self {
        NodesTree {
            root
        }
    }
    pub async fn process_request(& self, sender: &RpcMessageSender, request: &RpcMessage) -> ProcessRequestResult  {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        let shv_path = request.shv_path().unwrap_or("");
        let (nd, path_rest) = self.find_node(shv_path)?;
        nd.process_request(sender,request, path_rest).await
    }

    fn find_node<'a, 'b>(&'a self, path: &'b str) -> crate::Result<(&'a TreeNode, &'b str)> {
        fn find_node<'a, 'b>(pnd: &'a TreeNode, path: &'b str) -> (&'a TreeNode, &'b str) {
            let (dir, rest) = utils::shv_path_cut_first(path);
            // info!("{} ---> {} + {}", path, dir, rest);
            if dir.is_empty() {
                return (pnd, rest);
            }
            match &pnd.children {
                Some(chd) => {
                    for (name, nd) in chd.iter() {
                        if name == dir {
                            return find_node(nd, rest);
                        }
                    }
                }
                None => {}
            }
            return (pnd, path);
        }
        let f = find_node(&self.root, path);
        return Ok(f);
    }
}

pub type RequestProcessorRefType = Arc<Mutex<dyn RequestProcessor>>;
pub struct TreeNode {
    pub processor: Option<RequestProcessorRefType>,
    pub children: Option<Vec<(String, TreeNode)>>,
}

impl TreeNode {
    pub fn new() -> Self {
        TreeNode {
            processor: None,
            children: None,
        }
    }
    pub fn add_child_node(&mut self, name: &str, nd: TreeNode) -> &mut Self {
        match self.children {
            None => { self.children = Some(Vec::new()); }
            _ => {}
        }
        let ch = self.children.as_mut().unwrap();
        ch.push((name.to_string(), nd));
        self
    }
    fn find_child(&self, name: &str) -> Option<&Self> {
        if let Some(v) = &self.children {
            for (chname, nd) in v {
                if chname == name {
                    return Some(nd);
                }
            }
        }
        None
    }
    async fn is_dir(&self) -> bool {
        if self.children.is_some() {
            return true
        }
        if let Some(processor) = &self.processor {
            let g = processor.lock().await;
            return g.is_dir().await;
        }
        return false;
    }
    async fn process_request(&self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        //info!("################### process_request path: {} {}", shv_path, request.to_cpon());
        if shv_path.is_empty() {
            // if whole shv path was used
            let method = request.method().ok_or("Method is empty")?;
            if method == "ls" {
                if let Some(children) = &self.children {
                    // if children exists
                    let mut lst = List::new();
                    for (name, nd) in children {
                        let e: List = vec![name.into(), nd.is_dir().await.into()];
                        lst.push(e.into());
                    }
                    return Ok(Some(lst.into()));
                }
                // if children does not exist, then processor should handle ls
                if let None = &self.processor {
                    return Ok(Some(List::new().into()));
                }
            }
            else if method == "dir" {
                if let None = &self.processor {
                    let DIR = MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() };
                    let LS =  MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() };
                    // if processor does not exists
                    let mut lst = List::new();
                    lst.push(DIR.to_rpcvalue(255));
                    if self.children.is_some() {
                        lst.push(LS.to_rpcvalue(255));
                    }
                    return Ok(Some(lst.into()));
                }
                // if processor does exist, then processor should handle ls
            }
        }
        if let Some(processor) = &self.processor {
            let mut g = processor.lock().await;
            return g.process_request(sender, request, shv_path).await;
        }
        Err(format!("Cannot handle rpc request on path: {}", shv_path).into())
    }
}

#[async_trait]
pub trait RequestProcessor: Send {
    async fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult;
    async fn is_dir(&self) -> bool;
}


