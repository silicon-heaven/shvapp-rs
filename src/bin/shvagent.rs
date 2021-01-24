use shvapp::{client, DEFAULT_PORT};

use std::time::Duration;
use structopt::StructOpt;
use tracing::{warn, info, debug};
use std::env;
use shvapp::client::{ConnectionParams, RpcMessageSender};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue, metamethod};
use chainpack::rpcmessage::{RpcError, RpcErrorCode};
use chainpack::metamethod::{MetaMethod};
use tokio::process::Command;
use shvapp::shvnode::{TreeNode, NodesTree, RequestProcessor, ProcessRequestResult};
use chainpack::rpcvalue::List;
use shvapp::shvfsnode::FSDirRequestProcessor;

#[derive(StructOpt, Debug)]
#[structopt(name = "shvagent-cli", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV Agent")]
struct Cli {
    #[structopt(name = "hostname", short = "-s", long = "--host", default_value = "127.0.0.1")]
    host: String,
    #[structopt(short = "-p", long = "--port", default_value = DEFAULT_PORT)]
    port: u16,
    #[structopt(short = "-u", long = "--user")]
    user: String,
    #[structopt(long = "--password")]
    password: String,
    #[structopt(long = "--device-id")]
    device_id: Option<String>,
    #[structopt(short = "-m", long = "--mount-point")]
    mount_point: Option<String>,
    #[structopt(short = "-V", long = "--verbose", help = "Verbose log")]
    verbose: bool,
}

// const DEFAULT_RPC_TIMEOUT_MSEC: u64 = 5000;

/// Entry point for CLI tool.
///
/// The `[tokio::main]` annotation signals that the Tokio runtime should be
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> shvapp::Result<()> {
    // Parse command line arguments
    let cli = Cli::from_args();

    // Enable logging
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    let sb = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env());
    if cli.verbose == true {
        sb.pretty().finish().try_init()?;
    } else {
        sb.finish().try_init()?;
    }

    let device_id = cli.device_id.unwrap_or("".into());
    // Get the remote address to connect to
    // let rpc_timeout = Duration::from_millis(DEFAULT_RPC_TIMEOUT_MSEC);
    let mut connection_params = ConnectionParams::new(&cli.host, cli.port, &cli.user, &cli.password);
    connection_params.device_id = device_id.to_string();
    connection_params.mount_point = cli.mount_point.unwrap_or("".to_string());

    let mut root = TreeNode {
        processor: Some(Box::new(DeviceNodeRequestProcessor {
            app_name: "ShvAgent".into(),
            device_id,
        })),
        children: None,
    };
    let home_dir = dirs::home_dir();
    if let Some(home_dir) = home_dir {
        let home_dir = home_dir.to_str().ok_or(format!("Cannot convert '{:?}' to string", home_dir))?;
        root.add_child_node("fs", TreeNode {
            processor: Some(Box::new(FSDirRequestProcessor {
                root: home_dir.into(),
            })),
            children: None,
        }
        );
    }
    let mut shv_tree = NodesTree::new(root);
    //let mut app_node = ShvAgentAppNode::new();
    //let dyn_app_node = (&mut app_node) as &dyn ShvNode;
    loop {
        // Establish a connection
        let connect_res = client::connect(&connection_params).await;
        match connect_res {
            Ok((mut client, mut connection)) => {
                tokio::spawn(async move {
                    info!("Spawning connection message loop");
                    match connection.exec().await {
                        Ok(_) => {
                            info!("Connection message loop finished Ok");
                        }
                        Err(e) => {
                            warn!("Connection message loop finished with error: {}", e);
                        }
                    }
                });
                match client.login(&connection_params).await {
                    Ok(_) => {
                        let ping_fut = client.call(RpcMessage::create_request(".broker/app", "ping", None));
                        match ping_fut.await {
                            Ok(resp) => {
                                info!("Ping response: {}", resp);
                            }
                            Err(e) => {
                                info!("Ping error: {}", e);
                            }
                        }
                        //let mut timeout_cnt = 0;
                        loop {
                            match client.receive().await {
                                Ok(msg) => {
                                    debug!("Message arrived: {}", msg);
                                    if msg.is_request() {
                                        let sender = client.as_sender();
                                        let ret_val = shv_tree.process_request(&sender,&msg);
                                        if let Ok(None) = ret_val {
                                            // ret val will be sent async in handler
                                        }
                                        else {
                                            match msg.prepare_response() {
                                                Ok(mut resp_msg) => {
                                                    match ret_val {
                                                        Ok(None) => {}
                                                        Ok(Some(rv)) => {
                                                            resp_msg.set_result(rv);
                                                            client.send(resp_msg).await?;
                                                        }
                                                        Err(e) => {
                                                            resp_msg.set_error(RpcError::new(RpcErrorCode::MethodCallException, &e.to_string()));
                                                            client.send(resp_msg).await?;
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!("Create response meta error: {}.", e);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Read message error: {}.", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        info!("Login error: {}", e);
                    }
                };
            }
            Err(e) => {
                warn!("connect to broker error: {}", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

struct DeviceNodeRequestProcessor {
    app_name: String,
    device_id: String,
}

impl RequestProcessor for DeviceNodeRequestProcessor {
    fn process_request(&mut self, sender: &RpcMessageSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        let method = request.method().ok_or("Empty method")?;
        if shv_path.is_empty() {
            if method == "dir" {
                let methods = [
                    MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                    MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                    MetaMethod { name: "appName".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                    MetaMethod { name: "deviceId".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::new("rd"), description: "".into() },
                    MetaMethod { name: "runCmd".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("wr"), description: "".into() },
                ];
                let mut lst = List::new();
                for mm in methods.iter() {
                    lst.push(mm.to_rpcvalue(255));
                }
                return Ok(Some(lst.into()));
            }
            if method == "appName" {
                return Ok(Some(RpcValue::from(&self.app_name)));
            }
            if method == "deviceId" {
                return Ok(Some(RpcValue::from(&self.device_id)));
            }
            if method == "runCmd" {
                let request = request.clone();
                // let shv_path = shv_path.to_string();
                let sender = sender.clone();
                tokio::spawn(async move {
                    async fn run_cmd(request: &RpcMessage) -> shvapp::Result<RpcValue> {
                        let params = request.params().ok_or("No params")?;
                        let cmd = if params.is_list() {
                            let params = params.as_list();
                            if params.is_empty() {
                                return Err("Param list is empty".into());
                            }
                            params[0].as_str()?
                        }
                        else if params.is_data() {
                            params.as_str()?
                        }
                        else {
                            return Err("Invalid params".into());
                        };
                        let output = Command::new(cmd)
                            //.args(args)
                            .output().await?;
                        let out: &[u8] = &output.stdout;
                        return Ok(RpcValue::from(out))
                    }
                    match request.prepare_response() {
                        Ok(mut resp_msg) => {
                            match run_cmd(&request).await {
                                Ok(rv) => { resp_msg.set_result(rv); }
                                Err(e) => { resp_msg.set_error(RpcError::new(RpcErrorCode::MethodCallException, &e.to_string())); }
                            }
                            match sender.send(resp_msg).await {
                                Ok(_) => {}
                                Err(e) => { warn!("Send response error: {}.", e); }
                            }
                        }
                        Err(e) => {
                            warn!("Create response error: {}.", e);
                        }
                    }
                });
                return Ok(None);
            }
        }
        Err(format!("Unknown method '{}' on path '{}'", method, shv_path).into())
    }

    fn is_dir(&self) -> bool {
        return false;
    }
}
