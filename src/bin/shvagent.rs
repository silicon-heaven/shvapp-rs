use shvapp::{client, DEFAULT_PORT};

use std::time::Duration;
use structopt::StructOpt;
use tracing::{warn, info, debug};
use std::env;
use shvapp::client::{ConnectionParams};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;
use chainpack::RpcMessage;

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

    // Get the remote address to connect to
    // let rpc_timeout = Duration::from_millis(DEFAULT_RPC_TIMEOUT_MSEC);
    let connection_params = ConnectionParams::new(&cli.host, cli.port, &cli.user, &cli.password);
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
                            info!("Connection message loop finished with error: {}", e);
                        }
                    }
                });
                match client.login(&connection_params).await {
                    Ok(_) => {
                        let mut ping = client.create_request();
                        let ping_fut = ping.exec(RpcMessage::new_request(".broker/app", "ping", None));
                        match ping_fut.await {
                            Ok(resp) => {
                                info!("Ping response: {}", resp);
                            }
                            Err(e) => {
                                info!("Ping error: {}", e);
                            }
                        }
                        let mut ntf = client.create_message_notifier();
                        loop {
                            //tokio::time::sleep(Duration::from_secs(1)).await;
                            match ntf.wait_for_message_timeout(Duration::from_secs(1)).await {
                                Ok(msg) => {
                                    debug!("Message arrived: {}", msg);
                                }
                                Err(e) => {
                                    debug!("No message arrived within last second: {}.", e);
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

