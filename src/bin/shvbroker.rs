use structopt::StructOpt;
use tokio::signal;
use shvapp::{server, DEFAULT_PORT};
use tracing::{info, Level};
use std::env;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::EnvFilter;

#[tokio::main]
pub async fn main() -> shvapp::Result<()> {
    let cli = Cli::from_args();
    // enable logging
    // see https://docs.rs/tracing for more info
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    //let format = tracing_subscriber::fmt::format().pretty();
    //let ts = tracing_subscriber::fmt::try_init();
    let sb = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env());
        // filter spans/events with level TRACE or higher.
        //.with_max_level(Level::DEBUG);
        //.pretty();
        //.finish();
    //let sb2;
    if cli.verbose == true {
        sb.pretty().finish().try_init()?;
    } else {
        sb.finish().try_init()?;
    }

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
    #[structopt(
        short = "-V", long = "--verbose",
        help = "Verbose log"
    )]
    verbose: bool,
}
