use chain::chain::start_irys_node;
use clap::{command, Parser};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value = "./database")]
    database: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // TODO: fix this, we used to await the reth node exit future but can't anymore
    // so we need another near-infinite blocking future
    start_irys_node(Default::default()).await?;
    std::thread::park();

    Ok(())
}
