use shvapp::{client, DEFAULT_PORT};

use std::time::Duration;
use structopt::StructOpt;
use tracing::{warn, info, debug};
use std::env;
use shvapp::client::{ConnectionParams};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue, metamethod};
use chainpack::rpcmessage::{RpcError, RpcErrorCode};
use chainpack::metamethod::{MetaMethod, Signature};
use std::process::Command;
use async_trait::async_trait;
use shvapp::shvnode::{TreeNode, ShvNode, NodesTree, MethodListShvNode};
use shvapp::shvfsnode::FileSystemDirNode;
use std::sync::Arc;
use tokio::sync::Mutex;

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
        name: String::new(),
        shv_node: Arc::new(Mutex::new(Box::new(create_shvagent_node(&device_id)))),
        // child_nodes: vec![Box::new(Shv)],
        child_nodes: vec![]
    };
    root.add_child_node(TreeNode {
            name: "fs".to_string(),
            shv_node: Arc::new(Mutex::new(Box::new(FileSystemDirNode::new("/tmp")))),
            child_nodes: vec![]
        });
    let shv_tree = NodesTree::new(root);
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
                                        match msg.prepare_response() {
                                            Ok(mut resp_msg) => {
                                                const true_async: bool = true;
                                                if true_async {
                                                    let fut = shv_tree.process_request(&msg);
                                                    let mut client = client.clone();
                                                    tokio::spawn(async move {
                                                        let ret_val = fut.await;
                                                        match ret_val {
                                                            Ok(rv) => {
                                                                resp_msg.set_result(rv);
                                                            }
                                                            Err(e) => {
                                                                resp_msg.set_error(RpcError::new(RpcErrorCode::MethodCallException, &e.to_string()));
                                                            }
                                                        }
                                                        match client.send(resp_msg).await {
                                                            Ok(_) => {
                                                                debug!("Send response OK");
                                                            }
                                                            Err(e) => {
                                                                warn!("Send response error: {}.", e);
                                                            }
                                                        }
                                                    });
                                                }
                                                else {
                                                    let ret_val = shv_tree.process_request(&msg).await;
                                                    match ret_val {
                                                        Ok(rv) => {
                                                            resp_msg.set_result(rv);
                                                        }
                                                        Err(e) => {
                                                            resp_msg.set_error(RpcError::new(RpcErrorCode::MethodCallException, &e.to_string()));
                                                        }
                                                    }
                                                    match client.send(resp_msg).await {
                                                        Ok(_) => {
                                                            debug!("Send response OK");
                                                        }
                                                        Err(e) => {
                                                            warn!("Send response error: {}.", e);
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Create response meta error: {}.", e);
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

type ShvAgentNodeData = ();
type ShvAgentNode = MethodListShvNode<ShvAgentNodeData>;
fn create_shvagent_node(device_id: &str) -> ShvAgentNode {
    let mut ret = ShvAgentNode::new_device((), "ShvAgent", device_id);
    ret.add_method(
        MetaMethod { name: "runCmd".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("cmd"), description: "params: [\"command\", [param1, ...], {\"envkey1\": \"envval1\"}, 1, 2], only the command is mandatory".into() },
        Box::new(move |data: &mut ShvAgentNodeData, params: Option<&RpcValue>| -> shvapp::Result<RpcValue> {
            let params = params.ok_or("no params specified")?.as_list();
            let cmd = params.get(0).ok_or("no command specified")?.as_str()?;
            let mut child = Command::new(cmd);
            if let Some(args) = params.get(1) {
                for arg in args.as_list() {
                    child.arg(arg.as_str()?);
                }
            }
            let output = child.output()?;
            // Await until the command completes
            //let output = output.await?;
            let out: &[u8] = &output.stdout;
            return Ok(RpcValue::new(out))
        })
    );
    ret
}