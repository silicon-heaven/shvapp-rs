use structopt::StructOpt;
use std::{env};
use std::time::Duration;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue, metamethod};

use chainpack::rpcmessage::{RpcError, RpcErrorCode};
use chainpack::rpcvalue::List;
use chainpack::metamethod::{MetaMethod};

use shvapp::{Connection, DEFAULT_PORT, shvjournal};
use shvapp::client::{ClientSender, ConnectionParams};
use shvapp::shvnode::{TreeNode, NodesTree, RequestProcessor, ProcessRequestResult};
use shvapp::shvfsnode::FSDirRequestProcessor;

use log::{warn, info, debug};

use async_std::{
    process::Command,
    // channel::{Receiver, Sender},
    // io::{stdin, BufReader, BufWriter},
    net::{TcpStream},
    // prelude::*,
    task,
    // future,
};
use shvlog::LogConfig;

#[derive(StructOpt, Debug)]
#[structopt(name = "shvagent", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV Agent")]
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
    #[structopt(short, long, help = "SHV journal directory, /tmp/shvjournal/shvagent if not specified")]
    journal_dir: Option<String>,
    #[structopt(long, default_value = "1M", help = "SHV journal file size limit")]
    journal_file_size: String,
    #[structopt(long, default_value = "100M", help = "SHV journal dir size limit")]
    journal_dir_size: String,
    #[structopt(short = "-v", long = "--verbose", help = "Log levels for targets, for example: rpcmsg:W or :T")]
    verbosity: Vec<String>,
    #[structopt(short, long, help = "Log levels for modules, for example: client:W or :T, default is :W if not specified")]
    debug: Vec<String>,
    #[structopt(short = "-e", long = "--export-dir", help = "Directory, which will be exported as 'fs' subnode")]
    export_dir: Option<String>,
}

// const DEFAULT_RPC_TIMEOUT_MSEC: u64 = 5000;
pub(crate) fn main() -> shvapp::Result<()> {
    task::block_on(try_main())
}

async fn try_main() -> shvapp::Result<()> {
    // Parse command line arguments
    let cli = Cli::from_args();

    let log_config = LogConfig::new(&cli.debug, &cli.verbosity);
    let verbosity_string = log_config.verbosity_string();
    let _log_handle = shvlog::init(log_config)?;
    /*
    if let Some(journal_dir) = cli.journal_dir {
        let options = shvjournal::Options {
            journal_dir: journal_dir.into(),
            dir_size_limit: 0,
            file_size_limit: 0,
        };
        let _journal = shvjournal::Journal::new(options);
    }
    */
    shvjournal::Journal::test();

    log::info!("=====================================================");
    log::info!("{} starting up!", std::module_path!());
    log::info!("=====================================================");
    log::info!("Verbosity levels: {}", verbosity_string);

    //log::error!("error");
    //log::warn!("warn");
    //log::info!("info");
    //log::debug!("debug");
    //log::trace!("trace");

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
    //let exported_dir = dirs::home_dir();
    if let Some(export_dir) = cli.export_dir {
        root.add_child_node("fs", TreeNode {
            processor: Some(Box::new(FSDirRequestProcessor {
                root: export_dir.into(),
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
        let addr = format!("{}:{}", connection_params.host, connection_params.port);
        info!("connecting to: {}", addr);
        let stream = TcpStream::connect(&addr).await?;
        info!("connected to: {}", addr);
        let (mut connection, mut client) = Connection::new(stream, connection_params.protocol);
        task::spawn(async move {
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
                if let Some(heartbeat_interval) = connection_params.heartbeat_interval {
                    client.spawn_ping_task(heartbeat_interval);
                }
                loop {
                    match client.receive_message().await {
                        Ok(msg) => {
                            //info!(target: "rpcmsg", "<== Message arrived: {}", msg);
                            if msg.is_request() {
                                let ret_val = shv_tree.process_request(&client.to_sender(),&msg);
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
                                                    debug!(target: "rpcmsg", "==> Sending response: {}", &resp_msg);
                                                    client.send_message(&resp_msg).await?;
                                                }
                                                Err(e) => {
                                                    resp_msg.set_error(RpcError::new(RpcErrorCode::MethodCallException, &e.to_string()));
                                                    debug!(target: "rpcmsg", "==> Sending error: {}", &resp_msg);
                                                    client.send_message(&resp_msg).await?;
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
                warn!("Login error: {}", e);
                task::sleep(Duration::from_secs(5)).await;
            }
        };
    }
}

struct DeviceNodeRequestProcessor {
    app_name: String,
    device_id: String,
}

impl RequestProcessor for DeviceNodeRequestProcessor {
    fn process_request(&mut self, client_sender: &ClientSender, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        let method = request.method().ok_or("Empty method")?;
        if shv_path.is_empty() {
            if method == "dir" {
                let methods = [
                    MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() },
                    //MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("bws"), description: "".into() },
                    MetaMethod { name: "appName".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::from("bws"), description: "".into() },
                    MetaMethod { name: "deviceId".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::IsGetter.into(), access_grant: RpcValue::from("rd"), description: "".into() },
                    MetaMethod { name: "runCmd".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::from("wr"), description: "".into() },
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
                let client_sender = client_sender.clone();
                task::spawn(async move {
                    async fn run_cmd(request: &RpcMessage) -> shvapp::Result<RpcValue> {
                        let params = request.params().ok_or("No params")?;
                        let cmd = if params.is_list() {
                            let params = params.as_list();
                            if params.is_empty() {
                                return Err("Param list is empty".into());
                            }
                            params[0].as_str()
                        }
                        else if params.is_string() {
                            params.as_str()
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
                            match client_sender.send_message(&resp_msg).await {
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
