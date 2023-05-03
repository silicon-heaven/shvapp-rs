use async_std::{io,prelude::*};
use structopt::StructOpt;
use std::env;
use chainpack::{RpcMessage, RpcMessageMetaTags, RpcValue};

use shvapp::{Connection, DEFAULT_PORT};
use shvapp::client::{LoginParams};

use log::{warn, info};
// use log::debug;

use async_std::{
    net::TcpStream,
    task,
};
use async_std::os::unix::net::UnixStream;
use chainpack::rpcframe::Protocol;
use shvlog::LogConfig;
use url::Url;
use percent_encoding::percent_decode;
use shvapp::AsyncRWBox;

#[derive(StructOpt, Debug)]
#[structopt(name = "shvcall", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV Call")]
struct Cli {
    #[structopt(name = "url", short = "-s", long = "--url", help = "Url to connect to, example tcp://localhost:3755, localsocket:path/to/socket")]
    url: String,
    #[structopt(short = "-p", long = "--path")]
    path: String,
    #[structopt(short = "-m", long = "--method")]
    method: String,
    #[structopt(short = "-a", long = "--params")]
    params: Option<String>,
    #[structopt(short = "-v", long = "--verbose", help = "Log levels for targets, for example: rpcmsg:W or :T")]
    verbosity: Vec<String>,
    #[structopt(short = "-d", help = "Log levels for modules, for example: client:W or :T, default is :W if not specified")]
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
    let url = Url::parse(&cli.url)?;
    let password = percent_decode(url.password().unwrap_or("").as_bytes()).decode_utf8()?;
    let login_params = LoginParams{
        user: url.username().to_string(),
        password: password.to_string(),
        heartbeat_interval: None,
        ..Default::default()
    };

    // Establish a connection
    let stream: AsyncRWBox = match url.scheme().as_bytes() {
        b"tcp" => {
            let host = format!("{}:{}", url.host().unwrap_or(url::Host::Domain("localhost")), url.port().unwrap_or(DEFAULT_PORT));
            info!("connecting to host: {}", host);
            Box::new(TcpStream::connect(&host).await?)
        }
        b"localsocket" => {
            let path = url.path();
            info!("connecting to local domain socket: {}", &path);
            Box::new(UnixStream::connect(&path).await?)
        }
        sch => {
            return Err(format!("Invalid scheme: {:?}", sch).into())
        }
    };
    info!("connected Ok");
    let (mut connection, mut client) = Connection::new(stream, Protocol::ChainPack);

    // This is necessary in order to receive messages
    task::spawn(async move {
        info!("Spawning connection message loop");
        if let Err(e) = connection.exec().await {
            warn!("Connection message loop finished with error: {}", e);
        } else {
            info!("Connection message loop finished Ok");
        }
    });

    info!("login params: {:?}", &login_params);
    client.login(&login_params).await?;

    let params = match cli.params {
        None => None,
        Some(p) => {
            match RpcValue::from_cpon(&p) {
                Ok(p) => Some(p),
                Err(e) => return Err(format!("Invalid SHV call parameter: {}, error: {}", &p, e).into()),
            }
        },
    };
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
