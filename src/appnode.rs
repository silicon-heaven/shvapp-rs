use chainpack::{RpcMessage, RpcValue, RpcMessageMetaTags};
use chainpack::rpcvalue::List;
use tracing::debug;
use chainpack::metamethod::MetaMethod;

pub struct AppNode {
    pub app_name: String,
    pub device_id: String,
    pub device_type: String,
    pub methods: Vec<MetaMethod>,
}

impl AppNode {
    pub async fn process_request(&self, request: &RpcMessage) -> crate::Result<RpcValue> {
        if !request.is_request() {
            return Err("Not request".into());
        }
        debug!("request: {}", request);
        // let rq_id = request.request_id().unwrap();
        let shv_path = request.shv_path().unwrap_or("");
        let shv_path_list = Self::split_shv_path(shv_path);
        if shv_path_list.is_empty() {
            let method = request.method().unwrap();
            debug!("method: {}", method);
            if method == "dir" {
                let mut method_pattern = "".to_string();
                let mut attrs_pattern = 0;
                if let Some(params) = request.params() {
                    let params = params.as_list();
                    if params.len() >= 1 {
                        method_pattern = params[0].as_str().to_string();
                    }
                    if params.len() >= 2 {
                        //debug!("param [1]: {}", params[1]);
                        attrs_pattern = params[1].as_u32();
                    }
                }
                debug!("method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
                let mut lst = Vec::new();
                for mm in &self.methods {
                    if method_pattern.is_empty() {
                        lst.push(mm.list_attributes(attrs_pattern as u8));
                    }
                    else if method_pattern == mm.name {
                        lst.push(mm.list_attributes(attrs_pattern as u8));
                        break;
                    }
                }
                debug!("dir: {:?}", lst);
                return Ok(RpcValue::new(lst));
            }
            if method == "ls" {
                return Ok(RpcValue::new(List::new()));
            }
            if method == "appName" {
                return Ok(RpcValue::new(&self.app_name))
            }
            if method == "deviceId" {
                return Ok(RpcValue::new(&self.device_id))
            }
            if method == "deviceType" {
                return Ok(RpcValue::new(&self.device_type))
            }
            return Err(format!("Invalid method: '{}' call", method).into())
        }
        return Err(format!("Invalid path: '{}' call", shv_path).into())
    }

    fn split_shv_path(path: &str) -> Vec<&str> {
        let v = path.split('/')
            .filter(|s| !(*s).is_empty())
            .collect();
        return v
    }
}