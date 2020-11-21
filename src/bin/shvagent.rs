use shvapp::{client, DEFAULT_PORT};

use bytes::Bytes;
use std::num::ParseIntError;
use std::str;
use std::time::Duration;
use structopt::StructOpt;
use tracing::field::debug;
use std::io::{ErrorKind, Error};
use tracing::{warn, info, debug};
use std::env;
use shvapp::client::LoginParams;

#[derive(StructOpt, Debug)]
#[structopt(name = "mini-redis-cli", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "Issue Redis commands")]
struct Cli {
    #[structopt(name = "hostname", short = "-s", long = "--host", default_value = "127.0.0.1")]
    host: String,
    #[structopt(short = "-u", long = "--user")]
    user: String,
    #[structopt(long = "--password")]
    password: String,
    #[structopt(name = "port", long = "--port", default_value = DEFAULT_PORT)]
    port: u16,
}

const DEFAULT_RPC_TIMEOUT_MSEC: u64 = 5000;

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
    // Enable logging
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    tracing_subscriber::fmt::try_init()?;

    // Parse command line arguments
    let cli = Cli::from_args();

    // Get the remote address to connect to
    let addr = format!("{}:{}", cli.host, cli.port);
    let rpc_timeout = Duration::from_millis(DEFAULT_RPC_TIMEOUT_MSEC);
    loop {
        info!("connecting to: {}", addr);
        // Establish a connection
        let client_res = client::connect(&addr).await;
        match client_res {
            Ok(mut client) => {
                info!("connected to: {}", addr);
                match client.login(&LoginParams::new(&cli.user, &cli.password)).await {
                    Ok(_) => {
                        loop {
                            //let frame_res = client.read_frame_timeout(rpc_timeout).await;
                            let frame_res = client.read_frame().await;
                            debug!(?frame_res);
                            match frame_res {
                                Ok(frame) => {
                                    todo!("impl");
                                }
                                Err(e) => {
                                    return Err(e.into())
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("login error: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("conn error: {}", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

