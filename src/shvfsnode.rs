use crate::shvnode::{RequestProcessor, RequestProcessorRefType, ProcessRequestResult};
use chainpack::metamethod::{MetaMethod, Signature};
use chainpack::{RpcValue, metamethod, RpcMessage, RpcMessageMetaTags};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::{io, fs};
use tracing::{warn, info, debug};
use crate::utils;
use crate::client::RpcMessageSender;
use chainpack::rpcvalue::List;

pub struct FSDirRequestProcessor {
    pub root: String,
}
impl FSDirRequestProcessor {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.into(),
        }
    }
    fn make_absolute_path(&self, path: &str) -> PathBuf {
        Path::new(&self.root).join(path)
    }
    // fn is_dir_empty(&self, path: &Path) -> bool {
    //     match path.read_dir() {
    //         Ok(mut it) => it.next().is_none(),
    //         _ => true,
    //     }
    // }
    fn children2(&self, path: &str) -> crate::Result<Vec<(String, bool)>> {
        let mut pb = self.make_absolute_path(path);
        if pb.is_dir() {
            let mut ret = Vec::new();
            for entry in pb.read_dir()? {
                if let Ok(entry) = entry {
                    pb.push(entry.file_name());
                    let fname = entry.file_name().into_string().unwrap_or_default();
                    let is_dir = pb.is_dir();
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
impl RequestProcessor for FSDirRequestProcessor {
    async fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        let method = request.method().ok_or("Empty method")?;
        if method == "dir" {
            //info!("DIR path: {} abs: {:?}", shv_path, self.make_absolute_path(shv_path));
            let DIR = MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() };
            let LS = MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() };
            let READ = MetaMethod { name: "size".into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("rd"), description: "Read file content".into() };
            let SIZE = MetaMethod { name: "read".into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("rd"), description: "Read file content".into() };
            let CD = MetaMethod { name: "cd".into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("wr"), description: "Change root directory".into() };
            let PWD = MetaMethod { name: "pwd".into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("rd"), description: "Current root directory".into() };
            let mut lst = List::new();
            lst.push(DIR.to_rpcvalue(255));
            let path = self.make_absolute_path(shv_path);
            if path.is_dir() {
                lst.push(LS.to_rpcvalue(255));
                if shv_path.is_empty() {
                    lst.push(CD.to_rpcvalue(255));
                    lst.push(PWD.to_rpcvalue(255));

                }
            } else {
                lst.push(READ.to_rpcvalue(255));
                lst.push(SIZE.to_rpcvalue(255));
            }
            return Ok(Some(lst.into()));
        }
        if method == "ls" {
            let lst;
            let path = self.make_absolute_path(shv_path);
            if path.is_dir() {
                lst = self.children2(shv_path)?.iter().map(|(name, is_dir)| {
                    let mut lst = List::new();
                    lst.push(name.into());
                    lst.push((*is_dir).into());
                    RpcValue::from(lst)
                }).collect();
            }
            else {
                lst = List::new();
            }
            return Ok(Some(lst.into()));
        }
        if method == "read" {
            let data = fs::read(self.make_absolute_path(shv_path))?;
            return Ok(Some(RpcValue::from(data)))
        }
        if method == "size" {
            let data = fs::metadata(self.make_absolute_path(shv_path))?.len();
            return Ok(Some(RpcValue::from(data)))
        }
        if shv_path.is_empty() {
            if method == "cd" {
                let dir = request.params().ok_or("illegal params")?.as_str()?;
                if !self.make_absolute_path(dir).is_dir() {
                    return Err(format!("Path '{}' is not dir.", dir).into())
                }
                self.root = dir.to_string();
                return Ok(Some(RpcValue::from(true)))
            }
            if method == "pwd" {
                return Ok(Some(RpcValue::from(&self.root)))
            }
        }
        Err(format!("Unknown method '{}' on path '{}'", method, shv_path).into())
    }

    async fn is_dir(&self) -> bool {
        return  true;
    }
}
