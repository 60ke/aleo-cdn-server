use {
    colored::*, parking_lot::RwLock, snarkos_node_cdn::load_blocks, snarkvm::{prelude::{block::Block, transactions::Transactions, Testnet3}, utilities::ToBytes}, 
    std::{
        error::Error, sync::Arc, time::Instant
    }, tokio::task,
    bytes::BytesMut,
};
use anyhow::{anyhow, bail, Result};
use snarkvm::prelude::{
    store::{cow_to_copied, ConsensusStorage},
    Deserialize,
    DeserializeOwned,
    Ledger,
    Network,
    Serialize,
};



const MAX_RETRIES: usize = 3; // 最大重试次数

async fn load_blocks_range(base_url: &str, start_height: u32, end_height: u32, blocks: Arc<RwLock<Vec<Block<Testnet3>>>>) -> Result<(), Box<dyn Error>> {
    for _ in 0..MAX_RETRIES {
        let blocks_clone = blocks.clone();
        let process = move |block: Block<Testnet3>| {
            blocks_clone.write().push(block);
            Ok(())
        };
        let result = load_blocks(base_url, start_height, Some(end_height), process).await;
        if result.is_ok() {
            return Ok(());
        }
        println!("Error loading blocks, retrying...");
    }
    Err("Failed to load blocks after multiple retries".into())
}





async fn cdn_height<const BLOCKS_PER_FILE: u32>(base_url: &str) -> Result<u32> {
    // A representation of the 'latest.json' file object.
    #[derive(Deserialize, Serialize, Debug)]
    struct LatestState {
        exclusive_height: u32,
        inclusive_height: u32,
        hash: String,
    }
    // Create a request client.
    let client = match reqwest::Client::builder().build() {
        Ok(client) => client,
        Err(error) => bail!("Failed to create a CDN request client: {error}"),
    };
    // Prepare the URL.
    let latest_json_url = format!("{base_url}/latest.json");
    // Send the request.
    let response = match client.get(latest_json_url).send().await {
        Ok(response) => response,
        Err(error) => bail!("Failed to fetch the CDN height: {error}"),
    };
    // Parse the response.
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => bail!("Failed to parse the CDN height response: {error}"),
    };
    // Parse the bytes for the string.
    let latest_state_string = match bincode::deserialize::<String>(&bytes) {
        Ok(string) => string,
        Err(error) => bail!("Failed to deserialize the CDN height response: {error}"),
    };
    // Parse the string for the tip.
    let tip = match serde_json::from_str::<LatestState>(&latest_state_string) {
        Ok(latest) => latest.exclusive_height,
        Err(error) => bail!("Failed to extract the CDN height response: {error}"),
    };
    // Decrement the tip by a few blocks to ensure the CDN is caught up.
    let tip = tip.saturating_sub(10);
    // Adjust the tip to the closest subsequent multiple of BLOCKS_PER_FILE.
    Ok(tip - (tip % BLOCKS_PER_FILE) + BLOCKS_PER_FILE)
}




#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    const TEST_BASE_URL: &str = "https://s3.us-west-1.amazonaws.com/testnet3.blocks/phase3";
    let height = cdn_height::<50>(TEST_BASE_URL).await.unwrap();
    println!("Height: {}", height);
    let blocks = Arc::new(RwLock::new(Vec::new()));
    let expected = 50;
    let num_groups = 2;
    let mut tasks = Vec::new();
    let mut redis_client = redis::Client::open("redis://127.0.0.1/").unwrap().get_async_connection().await?;

    for i in 1..num_groups {
        let start_height = i * expected;
        let end_height = (i + 1) * expected;
        let blocks_clone = blocks.clone();
        let task = task::spawn(async move {
            load_blocks_range(TEST_BASE_URL, start_height, end_height, blocks_clone).await.unwrap();
        });
        tasks.push(task);
    }    
    let mut total:usize = 0;
    for task in tasks {
        task.await?;
        // let mut redis_conn = redis_client.

        for block in blocks.read().iter() {
            println!("Block height: {}", block.height());
            use bytes::BufMut;
            let mut bytes = BytesMut::default().writer();
            let _ = block.write_le(&mut bytes);
            let byte  = bytes.into_inner();
            total += byte.len();
            println!("Block size: {}", byte.len().to_string().green());
            let mut redis_cmd = redis::cmd("SET");
            let block_bytes = byte.freeze();
            let bytes_slice: &[u8] = &block_bytes;
            redis_cmd.arg(format!("block:{}", block.height())).arg(bytes_slice);
            let _:() = redis_cmd.query_async(&mut redis_client).await?;            
        }        
    }

    println!("Blocks loaded in {} seconds", start.elapsed().as_secs().to_string().red());
    println!("Loaded {} blocks", blocks.read().len().to_string().green());



    println!("Elapsed: {} seconds", start.elapsed().as_secs());
    println!("Total size: {}KB", (total/1024).to_string().green());
    Ok(())

}
