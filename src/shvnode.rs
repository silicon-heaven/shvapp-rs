use chainpack::metamethod::{MetaMethod};
use chainpack::rpcvalue::List;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags, metamethod};
use crate::utils;
use crate::client::RpcMessageSender;
use tracing::{debug};

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
    pub fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage) -> ProcessRequestResult  {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        let shv_path = request.shv_path().unwrap_or("");
        //let (nd, path_rest) = self.find_node(shv_path)?;
        self.root.process_request(sender,request, shv_path)
    }

    // fn find_node<'a, 'b>(&'a mut self, path: &'b str) -> crate::Result<(&'a mut TreeNode, &'b str)> {
    //     fn find_node<'a, 'b>(pnd: &'a mut TreeNode, path: &'b str) -> (&'a mut TreeNode, &'b str) {
    //         let (dir, rest) = utils::shv_path_cut_first(path);
    //         // info!("{} ---> {} + {}", path, dir, rest);
    //         if dir.is_empty() {
    //             return (pnd, rest);
    //         }
    //         {
    //             match &mut pnd.children {
    //                 Some(chd) => {
    //                     for (name, nd) in chd.iter_mut() {
    //                         if name == dir {
    //                             return (nd, rest);
    //                         }
    //                     }
    //                 }
    //                 None => {}
    //             }
    //         }
    //         return (pnd, path);
    //     }
    //     let f = find_node(&mut self.root, path);
    //     return Ok(f);
    // }
}

pub type RequestProcessorRefType = Box<dyn RequestProcessor>;
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
    // fn find_child(&self, name: &str) -> Option<&Self> {
    //     if let Some(v) = &self.children {
    //         for (chname, nd) in v {
    //             if chname == name {
    //                 return Some(nd);
    //             }
    //         }
    //     }
    //     None
    // }
    fn is_dir(&self) -> bool {
        if self.children.is_some() {
            return true
        }
        if let Some(processor) = &self.processor {
            return processor.is_dir();
        }
        return false;
    }
    fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        //info!("################### process_request path: {} {}", shv_path, request.to_cpon());
        if shv_path.is_empty() {
            // if whole shv path was used
            let method = request.method().ok_or("Method is empty")?;
            if method == "ls" {
                if let Some(children) = &self.children {
                    // if children exists
                    let mut lst = List::new();
                    for (name, nd) in children {
                        let e: List = vec![name.into(), nd.is_dir().into()];
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
                #[allow(non_snake_case)]
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
        let (dir, rest) = utils::shv_path_cut_first(shv_path);
        match &mut self.children {
            Some(chd) => {
                for (name, nd) in chd.iter_mut() {
                    if name == dir {
                        return nd.process_request(sender, request, rest);
                    }
                }
            }
            None => {}
        }
        if let Some(processor) = &mut self.processor {
            return processor.process_request(sender, request, shv_path);
        }
        Err(format!("Cannot handle rpc request on path: {}", shv_path).into())
    }
}

pub trait RequestProcessor: Send {
    fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult;
    fn is_dir(&self) -> bool;
}


