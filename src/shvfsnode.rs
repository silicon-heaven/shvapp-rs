use crate::shvnode::ShvNode;
use chainpack::metamethod::{MetaMethod, Signature};
use chainpack::{RpcValue, metamethod};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::{io, fs};
use tracing::{warn, info, debug};

pub struct FileSystemDirNode {
    root: String,
    methods: Vec<MetaMethod>,
}
impl FileSystemDirNode {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.into(),
            methods: vec![
                MetaMethod { name: "read".into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("rd"), description: "Read file content".into() },
            ]
        }
    }
    fn make_absolute_path(&self, path: &[&str]) -> PathBuf {
        let mut pb = PathBuf::new();
        pb.push(&self.root);
        for p in path {
            pb.push(p);
        }
        pb
    }
    fn is_dir_empty(&self, path: &Path) -> bool {
        match path.read_dir() {
            Ok(mut it) => it.next().is_none(),
            _ => true,
        }
    }
    fn children2(&self, path: &[&str]) -> crate::Result<Vec<(String, bool)>> {
        let mut pb = self.make_absolute_path(path);
        if pb.is_dir() {
            let mut ret = Vec::new();
            for entry in pb.read_dir()? {
                if let Ok(entry) = entry {
                    pb.push(entry.file_name());
                    let fname = entry.file_name().into_string().unwrap_or_default();
                    let is_dir = self.is_dir_empty(&pb);
                    debug!("------------------------------------------- {} is dir: {}", fname, is_dir);
                    let e = (fname, is_dir);
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
impl ShvNode for FileSystemDirNode {
    async fn dir<'a>(&'a self, path: &'_[&str]) -> crate::Result<Vec<&'a MetaMethod>> {
        if !self.make_absolute_path(path).is_dir() {
            return Ok(self.methods.iter().map(|mm: &MetaMethod| { mm }).collect())
        }
        return Ok(Vec::new())
    }

    async fn ls(&self, path: &[&str]) -> crate::Result<Vec<(String, bool)>> {
        return self.children2(path);
    }

    async fn call_method(&mut self, path: &[&str], method: &str, _params: Option<&RpcValue>) -> crate::Result<RpcValue> {
        if method == "read" {
            let data = fs::read(self.make_absolute_path(path))?;
            return Ok(RpcValue::new(data))
        }
        Err(format!("Unknown method {}", method).into())
    }
}

