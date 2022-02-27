use std::collections::{BTreeMap, BTreeSet};
use flexi_logger::AdaptiveFormat::Default;
use chainpack::{RpcValue, RpcMessage, RpcMessageMetaTags};
use log::{debug};
use crate::client::{ClientSender};

pub type ProcessRequestResult = crate::Result<Option<RpcValue>>;
pub trait RequestProcessor {
    fn process_request(&mut self, client: &ClientSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult;
    //fn is_dir(&self) -> bool;
}
pub type Node = Box<dyn RequestProcessor>;
type NodeMap = BTreeMap<String, Node>;

pub struct ShvTree {
    pub nodemap: NodeMap,
}
impl ShvTree {
    pub fn new() -> Self {
        ShvTree {
            nodemap: BTreeMap::new()
        }
    }
    pub fn add_node(&mut self, path: &str, node: Node) {
        self.nodemap.insert(path.into(), node);
    }
    pub fn ls(&self, path: &str) -> Vec<String> {
        let mut dirs: BTreeSet<String> = BTreeSet::new();
        let mut parent_dir = path.to_string();
        if !parent_dir.is_empty() {
            parent_dir.push('/');
        }
        for (key, _) in self.nodemap.range(parent_dir.clone() ..) {
            if key.starts_with(&parent_dir) {
                if let Some(dir) = key[parent_dir.len()..].split('/').next() {
                    dirs.insert(dir.to_string());
                }
            } else {
                break;
            }
        }
        dirs.into_iter().collect()
    }
    pub fn process_request(&mut self, client: &ClientSender, request: &RpcMessage) -> ProcessRequestResult  {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        let mut node_processor_path = String::new();
        let mut node_dir_path = request.shv_path().unwrap_or("").to_string();
        loop {
            if let Some(node) = self.nodemap.get_mut(&node_dir_path) {
                let result = node.process_request(client, request, &node_processor_path);
                return result;
            }
            if node_dir_path.is_empty() {
                break;
            }
            if let Some(ix) = node_dir_path.rfind('/') {
                let dir = (&node_dir_path[ix +  1 ..]).to_string();
                if node_processor_path.is_empty() {
                    node_processor_path = dir;
                } else {
                    node_processor_path += "/";
                    node_processor_path += &dir;
                }
                node_dir_path = (&node_dir_path[0 .. ix]).to_string();
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
    use crate::shvtree::{ProcessRequestResult, RequestProcessor, ShvTree};

    struct TestNode {}

    impl RequestProcessor for TestNode {
        fn process_request(&mut self, client: &ClientSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
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



