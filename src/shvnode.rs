use chainpack::metamethod::{MetaMethod, LsAttribute};
use chainpack::rpcvalue::List;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags, metamethod};
use tracing::debug;
use async_trait::async_trait;
use crate::utils;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use parking_lot::Mutex;

pub struct NodesTree {
    pub root: TreeNode,
}
impl NodesTree {
    pub fn new(root: TreeNode) -> Self {
        NodesTree {
            root,
        }
    }
    pub async fn process_request(&mut self, request: &RpcMessage) -> crate::Result<RpcValue>  {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        let shv_path = request.shv_path().unwrap_or("");
        let path = utils::split_shv_path(shv_path);
        debug!("path: {:?}", path);
        let method = request.method().ok_or("Method is empty")?;
        debug!("method: {}", method);
        let nd = self.cd(&path)?;
        let path = &path[nd.1 ..];
        let nd = nd.0;
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
            return nd.call_method(path, method, params).await;
        }
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
                }
                None => {
                    return Ok((pnd.clone(), ix))
                }
            }
        }
        return Ok((&self.root, 0))
    }
}
//type ChildNodeRefType = Arc<Mutex<TreeNode>>;
type ShvNodeRefType = Arc<Mutex<Box<dyn ShvNode>>>;
pub struct TreeNode {
    pub name: String,
    pub shv_node: ShvNodeRefType,
    pub child_nodes: Vec<TreeNode>,
}

impl TreeNode {
    pub fn new(name: &str, shv_node: Box<dyn ShvNode>) -> Self {
        TreeNode {
            name: name.to_string(),
            shv_node: Arc::new(Mutex::new(shv_node)),
            child_nodes: Vec::new(),
        }
    }
    pub fn add_child_node(&mut self, nd: TreeNode) -> &mut Self {
        self.child_nodes.push(nd);
        self
    }
    pub fn is_leaf(&self) -> bool {
        if !self.child_nodes.is_empty() {
            return false;
        }
        let g = self.shv_node.lock();
        if !g.is_leaf() {
            return false;
        }
        return true;
    }
    fn dir(& self, path: &[&str], method_pattern: &str, attrs_pattern: u32) -> crate::Result<RpcValue> {
        debug!("dir method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
        let mut lst: List = Vec::new();
        let g = self.shv_node.lock();
        for mm in g.dir(path)? {
            if method_pattern.is_empty() {
                lst.push(mm.dir_attributes(attrs_pattern as u8));
            }
            else if method_pattern == mm.name {
                lst.push(mm.dir_attributes(attrs_pattern as u8));
                break;
            }
        }
        debug!("dir: {:?}", lst);
        return Ok(RpcValue::new(lst));
    }
    fn ls(& self, path: &[&str], name_pattern: &str, ls_attrs_pattern: u32) -> crate::Result<RpcValue> {
        let with_children_info = (ls_attrs_pattern & (LsAttribute::HasChildren as u32)) != 0;
        debug!("ls name_pattern: {}, with_children_info: {}", name_pattern, with_children_info);
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
        let g = self.shv_node.lock();
        if !g.is_leaf() {
            for i in g.ls(path)? {
                if filter(&i.0, i.1) {
                    lst.push(map(&i.0, i.1));
                }
            }
        }
        for nd in self.child_nodes.iter() {
            if filter(&nd.name, g.is_leaf()) {
                lst.push(map(&nd.name, nd.is_leaf()));
            }
        }
        // debug!("dir: {:?}", lst);
        return Ok(RpcValue::new(lst));
    }
    async fn call_method(&self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        let mut g = self.shv_node.lock();
        for m in g.dir(path)?.iter() {
            // TDDO: check access rights
            if m.name == method {
                return g.call_method(&path, method, params).await
            }
        }
        Err(format!("Unknown method: {} on path: {:?}", method, path).into())
    }
}

#[async_trait]
pub trait ShvNode: Send + Sync {
    fn dir<'a>(&'a self, path: &'_[&str]) -> crate::Result<Vec<&'a MetaMethod>>;
    fn ls(&self, path: &[&str]) -> crate::Result<Vec<(String, bool)>> {
        Ok(Vec::new())
    }
    fn is_leaf(&self) -> bool {
        match self.ls(&[]) {
            Ok(v) => v.is_empty(),
            Err(_e) => true,
        }
    }
    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue>;
}

pub type MethodHandler<D> = Box<dyn Fn(&mut D, Option<&RpcValue>) -> crate::Result<RpcValue> + Send + Sync>;
pub struct MethodListShvNode<D> {
    data: D,
    methods: Vec<(MetaMethod, MethodHandler<D>)>
}
impl<D: Sized + Send + Sync> MethodListShvNode<D> {
    pub fn new(data: D) -> Self {
        let mut ret = Self {
            data,
            methods: Vec::new(),
        };
        ret.add_method(
            MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            Box::new(|_data: &mut D, _params: Option<&RpcValue>| -> crate::Result<RpcValue> { Err("dir should not be called directly".into()) }),
        );
        ret.add_method(
            MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            Box::new(|_data: &mut D, _params: Option<&RpcValue>| -> crate::Result<RpcValue> { Err("ls should not be called directly".into()) })
        );
        ret
    }
    pub fn new_device(data: D, app_name: &str, device_id: &str) -> Self {
        let mut ret = Self::new(data);
        let app_name = RpcValue::new(app_name);
        ret.add_method(
            MetaMethod { name: "appName".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            Box::new(move |_data: &mut D, _params: Option<&RpcValue>| -> crate::Result<RpcValue> { Ok(app_name.clone()) }),
        );
        let device_id = RpcValue::new(device_id);
        ret.add_method(
            MetaMethod { name: "deviceId".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            Box::new(move |data: &mut D, params: Option<&RpcValue>| -> crate::Result<RpcValue> { Ok(device_id.clone()) })
        );
        ret
    }
    pub fn add_method(&mut self, meta_method: MetaMethod, handler: MethodHandler<D>) {
        let mm = (meta_method, handler);
        self.methods.push(mm);
    }
}
#[async_trait]
impl <D: Sized + Send + Sync> ShvNode for MethodListShvNode<D> {
    fn dir<'a>(&'a self, path: &[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        let ret: Vec<&MetaMethod> =  self.methods.iter().map(|m| &m.0).collect();
        Ok(ret)
    }
    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        if path.is_empty() {
            return Err(format!("Path {:?} should be empty", path).into());
        }
        for mm in self.methods.iter() {
            if method == mm.0.name {
                let hnd = &mm.1;
                return hnd(&mut self.data, params);
            }
        }
        return Err(format!("Method {} not found", method).into());
    }
}