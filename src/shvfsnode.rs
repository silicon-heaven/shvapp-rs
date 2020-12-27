use crate::shvnode::RpcProcessor;
use chainpack::metamethod::{MetaMethod, Signature};
use chainpack::{RpcValue, metamethod};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::{io, fs};

pub struct FileSystemDirNode {
    root: String,
    methods: Vec<MetaMethod>,
}
impl FileSystemDirNode {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.into(),
            methods: vec![
                // MetaMethod { name: "dir".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                // MetaMethod { name: "ls".into(), signature: Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
            ]
        }
    }
    fn is_dir_empty(&self, path: &Path) -> bool {
        match path.read_dir() {
            Ok(mut it) => it.next().is_none(),
            _ => true,
        }
    }
    fn children2(&self, path: &[&str]) -> io::Result<Vec<(String, Option<bool>)>> {
        let mut pb = PathBuf::new();
        pb.push(&self.root);
        for p in path {
            pb.push(p);
        }
        if pb.is_dir() {
            let mut ret = Vec::new();
            for entry in pb.read_dir()? {
                if let Ok(entry) = entry {
                    pb.push(entry.file_name());
                    let fname = entry.file_name().into_string().unwrap_or_default();
                    let e = (fname, Some(self.is_dir_empty(&pb)));
                    pb.pop();
                    ret.push(e);
                }
            }
            return Ok(ret);
        }
        return Ok(Vec::new());
    }
}

#[async_trait]
impl RpcProcessor for FileSystemDirNode {
    fn methods(&self, path: &[&str]) -> Vec<&'_ MetaMethod> {
        if path.is_empty() {
            return self.methods.iter().map(|mm: &MetaMethod| { mm }).collect()
        }
        return Vec::new()
    }
    fn children(&self, path: &[&str]) -> Vec<(String, Option<bool>)> {
        match self.children2(path) {
            Ok(v) => v,
            _ => Vec::new(),
        }
    }

    async fn call_method(&mut self, path: &[&str], method: &str, params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        unimplemented!()
    }
}