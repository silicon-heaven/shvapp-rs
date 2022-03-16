use std::collections::{BTreeMap};
use async_std::channel::{Receiver, Sender};
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags, List};
use log::{debug};
use chainpack::metamethod::{Flag, MetaMethod, Signature};

pub type ProcessRequestResult = crate::Result<Option<RpcValue>>;
pub trait ShvNode {
    fn process_request(&mut self, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult;
    //fn is_dir(&self) -> bool;
    //fn set_rpc_response_sender(&mut self, sender: RpcResponseSender);
}
pub type RpcResponseSender = Sender<RpcMessage>;
pub type ShvNodeRef = Box<dyn ShvNode>;
type NodeMap = BTreeMap<String, ShvNodeRef>;

pub struct ShvNodeHelper {
    //node_id: String,
    //request_processor: ShvNodeRef,
}
impl ShvNodeHelper {
    pub fn new_method_dir() -> MetaMethod {
        MetaMethod {
            name: "dir".into(),
            signature: Signature::RetParam,
            flags: Flag::None.into(),
            access_grant: RpcValue::from("bws"),
            description: "dir() or dir(method_name) or dir([method_name, attributes]), calling dir() is the same as calling dir([\"\", 0])".into()
        }
    }
    fn parse_params(params: Option<&RpcValue>) -> (&str, u8) {
        if let Some(params) = params {
            let params = params.as_list();
            if params.is_empty() { ("", 0) }
            else if params.len() == 1 { (params[0].as_str(), 0) }
            else { (params[0].as_str(), params[1].as_int() as u8) }
        } else {
            ("", 0)
        }
    }
    pub fn dir_result<'a>(methods: impl Iterator<Item = &'a MetaMethod>, params: Option<&RpcValue>) -> RpcValue {
        let (name, attrs) = ShvNodeHelper::parse_params(params);
        let mut lst = List::new();
        for method in methods {
            if name.is_empty() || name == method.name {
                lst.push(method.to_rpcvalue(attrs));
            }
        }
        lst.into()
    }
    pub fn new_method_ls() -> MetaMethod {
        MetaMethod {
            name: "ls".into(),
            signature: Signature::RetParam,
            flags: Flag::None.into(),
            access_grant: RpcValue::from("bws"),
            description: "ls() or ls([\"\", attributes]), calling ls() is the same as calling ls([\"\", 0])".into()
        }
    }
    pub fn ls_result<'a>(dirs: impl Iterator<Item = (&'a String, &'a bool)>, params: Option<&RpcValue>) -> RpcValue {
        let (name, attrs) = ShvNodeHelper::parse_params(params);
        let mut lst = List::new();
        for (dir_name, is_dir) in dirs {
            if name.is_empty() || name == dir_name {
                if attrs == 0 {
                    lst.push(dir_name.into());
                } else {
                    let dir_lst: List = vec![dir_name.into(), is_dir.into()];
                    lst.push(dir_lst.into());
                }
            }
        }
        lst.into()
    }
}

pub struct ShvTree {
    pub nodemap: NodeMap,
    pub response_sender: RpcResponseSender,
    pub response_receiver: Receiver<RpcMessage>,
}
impl ShvTree {
    pub fn new() -> Self {
        let (response_sender, response_receiver) = async_std::channel::bounded(10);
        ShvTree {
            nodemap: BTreeMap::new(),
            response_sender,
            response_receiver,
        }
    }
    pub fn add_node(&mut self, path: &str, node: ShvNodeRef) {
        //let mut node = node;
        //node.set_rpc_response_sender(self.response_sender.clone());
        self.nodemap.insert(path.into(), node);
    }
    fn ls(&self, path: &str) -> Option<BTreeMap<String, bool>> {
        let mut dirs: BTreeMap<String, bool> = BTreeMap::new();
        let mut parent_dir = path.to_string();
        if !parent_dir.is_empty() {
            parent_dir.push('/');
        }
        let mut dir_exists = false;
        for (key, _) in self.nodemap.range(parent_dir.clone() ..) {
            if key.starts_with(&parent_dir) {
                dir_exists = true;
                let mut updirs = key[parent_dir.len()..].split('/');
                if let Some(dir) = updirs.next() {
                    let dirname = dir.to_string();
                    let has_children = updirs.next().is_some();
                    if let Some(val) = dirs.get_mut(&dirname) {
                        if !*val && has_children {
                            *val = true;
                        }
                    } else {
                        dirs.insert(dirname, has_children);
                    }
                }
            } else {
                break;
            }
        }
        if dir_exists {
            Some(dirs)
        } else {
            None
        }
    }
    pub fn is_leaf(&self, path: &str) -> Option<bool> {
        if let Some(dirs) = self.ls(path) {
            Some(dirs.is_empty())
        } else {
            None
        }
    }
    pub fn process_request(&mut self, request: &RpcMessage) -> ProcessRequestResult  {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        let method = request.method().unwrap_or("");
        let shv_path = request.shv_path().unwrap_or("");
        let mut after_slash_ix = 0;
        loop {
            let node_dir_path;
            let node_processor_path;
            let path = &shv_path[after_slash_ix..];
            if let Some(ix) = path.find('/') {
                after_slash_ix += ix + 1;
                node_dir_path = &shv_path[.. (after_slash_ix-1)];
                node_processor_path = &shv_path[after_slash_ix ..];
            } else {
                node_dir_path = shv_path;
                node_processor_path = "";
            }
            if let Some(node) = self.nodemap.get_mut(&node_dir_path.to_string()) {
                let result = node.process_request(request, &node_processor_path);
                return result;
            }
            if node_processor_path.is_empty() {
                break;
            }
        }
        if let Some(dirs) = self.ls(shv_path) {
            if method == "ls" {
                return Ok(Some(ShvNodeHelper::ls_result(dirs.iter(), request.params())));
            }
            if method == "dir" {
                let dir = ShvNodeHelper::new_method_dir();
                let ls = ShvNodeHelper::new_method_ls();
                let methods = vec![dir, ls];
                return Ok(Some(ShvNodeHelper::dir_result(methods.iter(), request.params())));
            }
        }
        Err(format!("Invalid request path: '{}'", request.shv_path().unwrap_or("INVALID")).into())
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

#[cfg(test)]
mod tests {
    use chainpack::RpcMessage;
    use crate::client::ClientSender;
    use crate::shvtree::{ProcessRequestResult, ShvNode, ShvTree};

    struct TestNode {}

    impl ShvNode for TestNode {
        fn process_request(&mut self, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
            Ok(Some(().into()))
        }
    }

    #[test]
    fn tst_ls() -> crate::Result<()> {
        let mut tree = ShvTree {
            nodemap: Default::default()
        };
        tree.add_node("a/b/c",Box::new(TestNode {}));
        tree.add_node("a/1/c",Box::new(TestNode {}));
        tree.add_node("a/2",Box::new(TestNode {}));
        tree.add_node("a",Box::new(TestNode {}));
        tree.add_node("c",Box::new(TestNode {}));
        tree.add_node("d/b/c1",Box::new(TestNode {}));
        tree.add_node("d/b/c2",Box::new(TestNode {}));
        assert_eq!(tree.ls(""), vec!["a", "c", "d"]);
        assert_eq!(tree.ls("a"), vec!["1", "2", "b"]);
        assert_eq!(tree.ls("a/b"), vec!["c"]);
        assert_eq!(tree.ls("d/b"), vec!["c1", "c2"]);
        assert!(tree.ls("e/b").is_empty());
        Ok(())
    }
}
/*
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
    fn process_request(&mut self, client: &ClientSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
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
                    let DIR = MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() };
                    let LS =  MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() };
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
                        return nd.process_request(client, request, rest);
                    }
                }
            }
            None => {}
        }
        if let Some(processor) = &mut self.processor {
            return processor.process_request(client, request, shv_path);
        }
        Err(format!("Cannot handle rpc request on path: {}", shv_path).into())
    }
}
*/



