use crate::shvnode::{RequestProcessor, ProcessRequestResult};
use chainpack::metamethod::{MetaMethod, Signature};
use chainpack::{RpcValue, metamethod, RpcMessage, RpcMessageMetaTags};
use chainpack::rpcvalue::List;
use std::path::{Path, PathBuf};
use sha1::{Sha1, Digest};
use std::{fs};
use log::{debug};
use crate::client::{ClientSender};

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
            ret.sort();
            return Ok(ret);
        }
        return Ok(Vec::new());
    }
}

impl RequestProcessor for FSDirRequestProcessor {
    fn process_request(&mut self, _client: &ClientSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        let method = request.method().ok_or("Empty method")?;
        const M_DIR: &str = "dir";
        const M_LS: &str = "ls";
        const M_READ: &str = "read";
        const M_READ_COMPRESSED: &str = "readCompressed";
        const M_SIZE: &str = "size";
        const M_HASH: &str = "hash";
        const M_CD: &str = "cd";
        const M_PWD: &str = "pwd";
        #[allow(non_snake_case)]
        if method == M_DIR {
            //info!("DIR path: {} abs: {:?}", shv_path, self.make_absolute_path(shv_path));
            let DIR = MetaMethod { name: M_DIR.into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() };
            let LS = MetaMethod { name: M_LS.into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() };
            let READ = MetaMethod { name: M_READ.into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("rd")
                , description: "Read file content".into() };
            let READ_COMPRESSED = MetaMethod { name: M_READ_COMPRESSED.into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("rd")
                , description: "Read file content compressed by LZ4".into() };
            let SIZE = MetaMethod { name: M_SIZE.into(), signature: Signature::RetVoid, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::from("rd"), description: "File content size".into() };
            let HASH = MetaMethod { name: M_HASH.into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("rd"), description: "File content SHA1".into() };
            let CD = MetaMethod { name: M_CD.into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("wr"), description: "Change root directory".into() };
            let PWD = MetaMethod { name: M_PWD.into(), signature: Signature::RetVoid, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("rd"), description: "Current root directory".into() };
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
                lst.push(SIZE.to_rpcvalue(255));
                lst.push(HASH.to_rpcvalue(255));
                lst.push(READ.to_rpcvalue(255));
                lst.push(READ_COMPRESSED.to_rpcvalue(255));
            }
            return Ok(Some(lst.into()));
        }
        if method == M_LS {
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
        if method == M_READ {
            let data = fs::read(self.make_absolute_path(shv_path))?;
            return Ok(Some(RpcValue::from(data)))
        }
        if method == M_READ_COMPRESSED {
            let data = fs::read(self.make_absolute_path(shv_path))?;
            let mut compressed: Vec<u8> = Vec::new();
            lz_fear::CompressionSettings::default()
                .compress(&data[..], &mut compressed)?;
            return Ok(Some(RpcValue::from(compressed)))
        }
        if method == M_SIZE {
            let data = fs::metadata(self.make_absolute_path(shv_path))?.len();
            return Ok(Some(RpcValue::from(data)))
        }
        if method == M_HASH {
            let data = fs::read(self.make_absolute_path(shv_path))?;
            let mut hasher = Sha1::new();
            hasher.update(&data);
            let result = hasher.finalize();
            let hex_string = hex::encode(&result);
            return Ok(Some(RpcValue::from(hex_string)))
        }
        if shv_path.is_empty() {
            if method == M_CD {
                let dir = request.params().ok_or("illegal params")?.as_str();
                if !self.make_absolute_path(dir).is_dir() {
                    return Err(format!("Path '{}' is not dir.", dir).into())
                }
                self.root = dir.to_string();
                return Ok(Some(RpcValue::from(true)))
            }
            if method == M_PWD {
                return Ok(Some(RpcValue::from(&self.root)))
            }
        }
        Err(format!("Unknown method '{}' on path '{}'", method, shv_path).into())
    }

    fn is_dir(&self) -> bool {
        return  true;
    }
}
