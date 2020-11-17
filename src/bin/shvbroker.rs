use structopt::StructOpt;
use tokio::signal;
use shapp::{server};

#[tokio::main]
pub async fn main() -> shapp::Result<()> {
    // enable logging
    // see https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let port = cli.port.unwrap_or(3755);
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
        help = "Plain socket port, set value to enable listening, example: -p 3755"
    )]
    port: Option<u16>,
}
