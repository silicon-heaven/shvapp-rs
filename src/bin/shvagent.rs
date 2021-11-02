use structopt::StructOpt;
use std::{env};
use std::collections::HashMap;
use std::time::Duration;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue, metamethod};

use chainpack::rpcmessage::{RpcError, RpcErrorCode};
use chainpack::rpcvalue::List;
use chainpack::metamethod::{MetaMethod};

use shvapp::{Connection, DEFAULT_PORT};
use shvapp::client::{Client, ConnectionParams};
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
use chrono::{NaiveDateTime, TimeZone};
use flexi_logger::{DeferredNow, Level, Logger, Record};
use flexi_logger::filter::{LogLineFilter, LogLineWriter};

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
    #[structopt(short = "-v", long = "--verbose", help = "Verbosity levels for targets, for example: rpcmsg:W or :T")]
    verbosity: Option<String>,
    #[structopt(short = "-e", long = "--export-dir", help = "Directory, which will be exported as 'fs' subnode")]
    export_dir: Option<String>,
}

// const DEFAULT_RPC_TIMEOUT_MSEC: u64 = 5000;
struct TopicVerbosity {
    default_level: log::Level,
    topic_levels: HashMap<String, log::Level>
}
impl TopicVerbosity {
    pub fn new(verbosity: &str) -> TopicVerbosity {
        let mut bc = TopicVerbosity {
            default_level: log::Level::Info,
            topic_levels: HashMap::new()
        };
        for level_str in verbosity.split(',') {
            let parts: Vec<&str> = level_str.split(':').collect();
            let (target, level_abbr) = if parts.len() == 1 {
                (parts[0], "T")
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                panic!("Cannot happen");
            };
            let level = match level_abbr {
                "D" => log::Level::Debug,
                "I" => log::Level::Info,
                "W" => log::Level::Warn,
                "E" => log::Level::Error,
                _ => log::Level::Trace,
            };
            if target.is_empty() {
                bc.default_level = level;
            } else {
                bc.topic_levels.insert(target.into(), level);
            }
        }
        bc
    }
    pub fn verbosity_to_string(&self) -> String {
        let s1 = format!(":{}", self.default_level);
        let s2 = self.topic_levels.iter()
            .map(|(target, level)| format!("{}:{}", target, level))
            .fold(String::new(), |acc, s| if acc.is_empty() { s } else { acc + "," + &s });
        format!("{},{}", s1, s2)
    }
}
impl LogLineFilter for TopicVerbosity {
    fn write(&self, now: &mut DeferredNow, record: &log::Record, log_line_writer: &dyn LogLineWriter) -> std::io::Result<()> {
        let level = if record.module_path().unwrap_or("") == record.target() {
            &self.default_level
        } else {
            match self.topic_levels.get(record.target()) {
                None => &self.default_level,
                Some(level) => level,
            }
        };
        if &record.level() <= level {
            log_line_writer.write(now, record)?;
        }
        Ok(())
    }
}

pub fn log_format(w: &mut dyn std::io::Write, now: &mut DeferredNow, record: &Record) -> Result<(), std::io::Error> {
    let sec = (now.now().unix_timestamp_nanos() / 1000_000_000) as i64;
    let nano = (now.now().unix_timestamp_nanos() % 1000_000_000) as u32;
    let ndt = NaiveDateTime::from_timestamp(sec, nano);
    let dt = chrono::Local.from_utc_datetime(&ndt);
    let level_abbr = match record.level() {
        Level::Error => "E",
        Level::Warn => "W",
        Level::Info => "I",
        Level::Debug => "D",
        Level::Trace => "T"
    };
    let target = if record.module_path().unwrap_or("") == record.target() { "".to_string() } else { format!("({})", record.target()) };
    write!(
        w,
        "{}[{}:{}]{}|{}| {}",
        dt.format("%Y-%m-%dT%H:%M:%S.%3f%z"),
        record.module_path().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        target,
        level_abbr,
        record.args()
    )
}

pub(crate) fn main() -> shvapp::Result<()> {
    task::block_on(try_main())
}

async fn try_main() -> shvapp::Result<()> {
    // Parse command line arguments
    let cli = Cli::from_args();

    let topic_verbosity = TopicVerbosity::new(&cli.verbosity.unwrap_or("".into()));
    let verbosity_string = topic_verbosity.verbosity_to_string();
    Logger::try_with_str("debug")?
        .filter(Box::new(topic_verbosity))
        .format(log_format)
        .start()?;
    log::info!("=====================================================");
    log::info!("{} starting up!", std::module_path!());
    log::info!("=====================================================");
    log::info!("Verbosity levels: {}", verbosity_string);

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
                /*
                let ping_fut = client.call(RpcMessage::create_request(".broker/app", "ping", None));
                match ping_fut.await {
                    Ok(resp) => {
                        info!("Ping response: {}", resp);
                    }
                    Err(e) => {
                        info!("Ping error: {}", e);
                    }
                }
                */
                loop {
                    match client.receive_message().await {
                        Ok(msg) => {
                            //info!(target: "rpcmsg", "<== Message arrived: {}", msg);
                            if msg.is_request() {
                                let ret_val = shv_tree.process_request(&client,&msg);
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
    fn process_request(&mut self, client: &Client, request: &RpcMessage, shv_path: &str) -> ProcessRequestResult {
        let method = request.method().ok_or("Empty method")?;
        if shv_path.is_empty() {
            if method == "dir" {
                let methods = [
                    MetaMethod { name: "dir".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
                    //MetaMethod { name: "ls".into(), signature: metamethod::Signature::RetParam, flags: metamethod::Flag::None.into(), access_grant: RpcValue::new("bws"), description: "".into() },
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
                let client = client.clone();
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
                            match client.send_message(&resp_msg).await {
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
