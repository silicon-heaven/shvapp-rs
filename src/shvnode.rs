use chainpack::metamethod::{MetaMethod, LsAttribute};
use chainpack::rpcvalue::List;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags, metamethod};
use tracing::{debug, warn};
use async_trait::async_trait;
use crate::utils;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use crate::client::RpcMessageSender;

pub type ShvResult = crate::Result<Option<RpcValue>>;

#[async_trait]
pub trait ShvNode {
    async fn is_dir(&self) -> Option<bool>;
    async fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, path: &str) -> ShvResult;
}

type ShvNodeRefType = Arc<Mutex<Box<dyn ShvNode>>>;
type MethodHandlerResult = Result<RpcValue, String>;
pub type MethodHandler = Fn(Option<RpcValue>) -> MethodHandlerResult;
pub struct ShvTreeNode {
    methods: Vec<(MetaMethod, MethodHandler)>,
    children: Vec<(String, ShvNodeRefType)>,
}
impl ShvTreeNode {
    pub fn new() -> Self {
        Self {
            methods: Vec::new(),
            children: Vec::new(),
        }
    }
    pub fn new_device(app_name: &str, device_id: &str) -> Self {
        let mut ret = Self::new();
        let app_name = RpcValue::from(app_name);
        ret.add_method(
            MetaMethod { name: "appName".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            Box::new(|params| -> MethodHandlerResult {
                Ok(app_name)
            }),
        );
        // let device_id = RpcValue::from(device_id);
        // ret.add_method(
        //     MetaMethod { name: "deviceId".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
        //     Box::new(|params| -> MethodHandlerResult {
        //         Ok(device_id)
        //     })
        // );
        ret
    }
    pub fn add_method(&mut self, meta_method: MetaMethod, handler: MethodHandler) {
        let mm = (meta_method, handler);
        self.methods.push(mm);
    }
    pub fn add_child_node(&mut self, name: &str, nd: Box<dyn ShvNode>) -> &mut Self {
        self.children.push((nd.name.clone(), Arc::new(Mutex::new(nd))));
        self
    }
}
#[async_trait]
impl ShvNode for ShvTreeNode {
    async fn is_dir(&self) -> Option<bool> {
        Some(!self.children.is_empty())
    }

    async fn process_request(&self, sender: &RpcMessageSender, request: &RpcMessage, path: &str) -> ShvResult {
        let shv_path = request.shv_path().unwrap_or("");
        let (node_name, path_rest) = utils::shv_path_cut_first(shv_path);
        if node_name.is_empty() {
            for (chname, chnd) in self.children.iter() {
                if chname == node_name {
                    let mut g = chnd.lock()?;
                    return g.process_request(sender, request, path_rest);
                }
            }
        }
        let method = request.method().ok_or("Method is empty")?;
        if "dir" == method {
        } else if "ls" == method {
            let method_pattern = "";
            let attrs_pattern = 255;
            let dir_ls: [&MetaMethod; 2] = [
                &MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                &MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            ];
            let mut lst: List = Vec::new();
            let it1 = dir_ls.iter();
            let it2 = self.methods.iter().map(|mm| &mm.0);
            for mm in it1.chain(it2) {
                if method_pattern.is_empty() {
                    lst.push(mm.dir_attributes(attrs_pattern as u8));
                }
                else if method_pattern == mm.name {
                    lst.push(mm.dir_attributes(attrs_pattern as u8));
                    break;
                }
            }
        } else {
            for (mm, hnd) in self.methods.iter() {
                if mm.name == method {

                }
            }
        }
        Err(format!("Unknown method: {} on path: {}", method, shv_path).into())
    }
}
/*
if !request.is_request() {
    return Err("Not request".into());
}
debug!("request: {}", request);
let shv_path = request.shv_path().unwrap_or("");
let path = utils::split_shv_path(shv_path);
debug!("path: {:?}", path);
// let method = request.method().ok_or("Method is empty")?;
// debug!("method: {}", method);
let nd_ix = self.cd(&path)?;
let nd = nd_ix.0;
let path = &path[nd_ix.1 ..];
request.set_shvpath(&utils::join_shv_path(path));
return nd.call_method(&request);
let params = request.params();
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
    return nd.dir(path, &method_pattern, attrs_pattern);
} else if method == "ls" {
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
    return nd.ls(path, &name_pattern, ls_attrs);
} else {
    return nd.call_method(path, method, params);
}
fn cd(&self, path: &[&str]) -> crate::Result<(&TreeNode, usize)> {
    fn find_node<'a>(pnd: &'a TreeNode, name: &'_ str) -> Option<&'a TreeNode> {
        for nd in pnd.child_nodes.iter() {
            if &nd.name == name {
                return Some(nd);
            }
        }
        None
    }
    let mut pnd = &self.root;
    let mut ix = 0;
    for p in path {
        let opnd = find_node(&pnd, p);
        match opnd {
            Some(nd) => {
                pnd = nd;
                ix += 1;
                if ix == path.len() {
                    return Ok((pnd, ix))
                }
            }
            None => {
                return Ok((pnd, ix))
            }
        }
    }
    return Ok((&self.root, 0))
}

*/

// pub struct TreeNode {
//     pub name: String,
//     pub shv_node: ShvNodeRefType,
//     pub child_nodes: Vec<TreeNode>,
// }
//
// impl TreeNode {
//     pub fn new(name: &str, shv_node: Box<dyn ShvNode>) -> Self {
//         TreeNode {
//             name: name.to_string(),
//             shv_node: Arc::new(Mutex::new(shv_node)),
//             child_nodes: Vec::new(),
//         }
//     }
    // fn dir(& self, path: &[&str], method_pattern: &str, attrs_pattern: u32) -> ShvResult<RpcValue> {
    //     debug!("=== dir === node: '{}', path: {:?}, method pattern: {}, attrs pattern: {}", self.name, path, method_pattern, attrs_pattern);
    //     let dir_ls: [&MetaMethod; 2] = [
    //         &MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
    //         &MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
    //     ];
    //     //let dir_ls = [&DIR_LS[0], &DIR_LS[1]];
    //     let g = self.shv_node.lock().unwrap();
    //     let dir = g.dir(path)?;
    //     let mut lst: List = Vec::new();
    //     let it = dir_ls.iter().chain(&dir);
    //     for mm in it {
    //         if method_pattern.is_empty() {
    //             lst.push(mm.dir_attributes(attrs_pattern as u8));
    //         }
    //         else if method_pattern == mm.name {
    //             lst.push(mm.dir_attributes(attrs_pattern as u8));
    //             break;
    //         }
    //     }
    //     debug!("--- dir ---: {:?}", lst);
    //     return Ok(RpcValue::new(lst));
    // }
    // fn ls(& self, path: &[&str], name_pattern: &str, ls_attrs_pattern: u32) -> ShvResult<RpcValue> {
    //     let with_children_info = (ls_attrs_pattern & (LsAttribute::HasChildren as u32)) != 0;
    //     debug!("=== ls === node: '{}', path: {:?}, name_pattern: {}, with_children_info: {}", self.name, path, name_pattern, with_children_info);
    //     let filter = |name: &str, _is_leaf: bool| {
    //         if !name_pattern.is_empty() {
    //             name_pattern == name
    //         } else {
    //             true
    //         }
    //     };
    //     let map = |name: &str, is_leaf: bool| -> RpcValue {
    //         if with_children_info {
    //             let mut lst = List::new();
    //             lst.push(RpcValue::new(name));
    //             lst.push(RpcValue::new(!is_leaf));
    //             RpcValue::new(lst)
    //         } else {
    //             RpcValue::new(name)
    //         }
    //     };
    //     let mut lst = List::new();
    //     let g = self.shv_node.lock().await;
    //     let is_leaf = g.is_leaf().await;
    //     if !is_leaf {
    //         for i in g.ls(path).await? {
    //             if filter(&i.0, i.1) {
    //                 lst.push(map(&i.0, i.1));
    //             }
    //         }
    //     }
    //     for nd in self.child_nodes.iter() {
    //         if filter(&nd.name, is_leaf) {
    //             lst.push(map(&nd.name, nd.is_leaf().await));
    //         }
    //     }
    //     debug!("--- ls ---: {:?}", lst);
    //     return Ok(RpcValue::new(lst));
    // }
    // fn call_method(&self, request: &RpcMessage) -> ShvResult<RpcValue> {
    //     let mut g = self.shv_node.lock().unwrap();
    //     let method = request.method().ok_or("Method is empty")?;
    //     debug!("method: {}", method);
    //     let path = request.shv_path().unwrap_or("");
    //     if method == "dir" {
    //         if let Some(mm) = g.dir(path)? {
    //             return Ok(Some(dir_to_rpcvalue(&mm)));
    //         }
    //     }
    //     else if method == "ls" {
    //         if self.child_nodes.is_empty() {
    //             if let Some(ls) = g.ls(path)? {
    //                 return Ok(Some(ls_to_rpcvalue(&ls)));
    //             }
    //         }
    //         else {
    //             let ls: Vec<(String, bool)> = self.child_nodes.iter().map(|nd| (nd.name.to_string(), nd.child_nodes.is_empty())).collect();
    //             return Ok(Some(ls_to_rpcvalue(&ls)));
    //         }
    //     }
    //     else {
    //         return g.call_method(&request);
    //         // for m in g.dir(path)?.iter() {
    //         //     // TDDO: check access rights
    //         //     if m.name == method {
    //         //         return g.call_method(&path, method, params).await
    //         //     }
    //         // }
    //     }
    //     Err(format!("Unknown method: {} on path: {:?}", method, path).into())
    // }
// }

fn dir_to_rpcvalue(dir: &[&MetaMethod]) -> RpcValue {
    //debug!("=== dir === node: '{}', path: {:?}, method pattern: {}, attrs pattern: {}", node_name, path, method_pattern, attrs_pattern);
    let method_pattern = "";
    let attrs_pattern = 255;
    let dir_ls: [&MetaMethod; 2] = [
        &MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
        &MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
    ];
    let mut lst: List = Vec::new();
    let it = dir_ls.iter().chain(dir);
    for mm in it {
        if method_pattern.is_empty() {
            lst.push(mm.dir_attributes(attrs_pattern as u8));
        }
        else if method_pattern == mm.name {
            lst.push(mm.dir_attributes(attrs_pattern as u8));
            break;
        }
    }
    debug!("--- dir ---: {:?}", lst);
    return lst.into();
}
fn ls_to_rpcvalue(ls: &[(String, bool)]) -> RpcValue {
    let with_children_info = true; //(ls_attrs_pattern & (LsAttribute::HasChildren as u32)) != 0;
    let name_pattern = "";
    //debug!("=== ls === node: '{}', path: {:?}, name_pattern: {}, with_children_info: {}", self.name, path, name_pattern, with_children_info);
    let filter = |name: &str, _is_leaf: bool| {
        if !name_pattern.is_empty() {
            name_pattern == name
        } else {
            true
        }
    };
    let map = |name: &str, is_leaf: bool| -> RpcValue {
        if with_children_info {
            let mut lst = List::new();
            lst.push(RpcValue::new(name));
            lst.push(RpcValue::new(!is_leaf));
            RpcValue::new(lst)
        } else {
            RpcValue::new(name)
        }
    };
    let mut lst = List::new();
    for (name, is_leaf) in ls.iter() {
        if filter(name, *is_leaf) {
            lst.push(map(name, *is_leaf));
        }
    }
    debug!("--- ls ---: {:?}", lst);
    return lst.into();
}

/*
fn dir(&self, path: &str) -> ShvResult<Vec<&MetaMethod>> {
    if path.is_empty() {
        let ret: Vec<&MetaMethod> =  self.methods.iter().map(|m| &m.0).collect();
        //warn!("{:?}", ret);
        return Ok(Some(ret))
        //return Err(format!("dir: path {:?} should be empty", path).into());
    }
    return Ok(Some(Vec::new()));
}

fn ls(&self, path: &str) -> ShvResult<Vec<(String, bool)>> {
    //if !path.is_empty() {
    //    return Err(format!("ls: path {:?} should be empty", path).into());
    //}
    Ok(Some(Vec::new()))
}

fn call_method(&mut self, request: &RpcMessage) -> ShvResult<RpcValue> {
    let method = request.method().ok_or("Method is empty")?;
    //debug!("method: {}", method);
    let path = request.shv_path().unwrap_or("");
    let params = request.params();
    if !path.is_empty() {
        return Err(format!("call_method: path {:?} should be empty", path).into());
    }
    for (name, handler) in self.methods.iter() {
        if method == name {
            return handler(&mut self.data, params);
        }
    }
    return Err(format!("Method {} not found", method).into());
}
 */