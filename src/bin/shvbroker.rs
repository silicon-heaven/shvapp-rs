use structopt::StructOpt;
use tokio::signal;
use shvapp::{server, DEFAULT_PORT};
use tracing::{info};
use std::env;

#[tokio::main]
pub async fn main() -> shvapp::Result<()> {
    // enable logging
    // see https://docs.rs/tracing for more info
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let port = cli.port;
    info!("Starting SHV Broker, listenning on port: {}", port);
    // Bind a TCP listener
    let listener = tokio::net::TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    server::run(listener, signal::ctrl_c()).await
}

#[derive(StructOpt, Debug)]
#[structopt(name = "mini-redis-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Redis server")]
struct Cli {
    #[structopt(
        name = "port",
        short = "-p", long = "--port",
        default_value = DEFAULT_PORT,
        help = "Plain socket port, set value to enable listening, example: -p DEFAULT_PORT"
    )]
    port: u16,
}
