use async_std::{io,prelude::*};
use structopt::StructOpt;
use std::env;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};

use shvapp::{Connection, DEFAULT_PORT};
use shvapp::client::ConnectionParams;

use log::{warn, info};
// use log::debug;

use async_std::{
    net::TcpStream,
    task,
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
    #[structopt(long = "--path")]
    path: String,
    #[structopt(long = "--method")]
    method: String,
    #[structopt(long = "--params")]
    params: Option<String>,
    #[structopt(short = "-v", long = "--verbose", help = "Log levels for targets, for example: rpcmsg:W or :T")]
    verbosity: Vec<String>,
    #[structopt(short, long, help = "Log levels for modules, for example: client:W or :T, default is :W if not specified")]
    debug: Vec<String>,
    #[structopt(short = "-x", long = "--chainpack", help = "Output as Chainpack instead of default CPON")]
    chainpack: bool,
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

    log::info!("=====================================================");
    log::info!("{} starting up!", std::module_path!());
    log::info!("=====================================================");
    log::info!("Verbosity levels: {}", verbosity_string);

    // let rpc_timeout = Duration::from_millis(DEFAULT_RPC_TIMEOUT_MSEC);
    let connection_params = ConnectionParams::new(&cli.host, cli.port, &cli.user, &cli.password);

    // Establish a connection
    let addr = format!("{}:{}", connection_params.host, connection_params.port);
    info!("connecting to: {}", addr);
    let stream = TcpStream::connect(&addr).await?;
    info!("connected to: {}", addr);
    let (mut connection, mut client) = Connection::new(stream, connection_params.protocol);

    // This is necessary in order to receive messages
    task::spawn(async move {
        info!("Spawning connection message loop");
        if let Err(e) = connection.exec().await {
            warn!("Connection message loop finished with error: {}", e);
        } else {
            info!("Connection message loop finished Ok");
        }
    });

    client.login(&connection_params).await?;

    let params = cli.params.and_then(|p| Some(RpcValue::from_cpon(&p).unwrap()));
    let msg = RpcMessage::create_request(&cli.path, &cli.method, params);
    let request_id = msg.request_id();
    client.send_message(&msg).await?;

    while let Ok(response) = client.receive_message().await {
        if response.request_id() == request_id {
            let mut stdout = io::stdout();
            let response_bytes = if cli.chainpack {
                response.as_rpcvalue().to_chainpack()
            } else {
                response.to_cpon().into_bytes()
            };
            stdout.write_all(&response_bytes).await?;
            stdout.flush().await?;
            break;
        }
    }
    Ok(())
}
