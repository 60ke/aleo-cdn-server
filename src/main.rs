use {
    anyhow::{anyhow, Result},
    parking_lot::RwLock,
    snarkos_node_cdn::load_blocks,
    snarkvm::prelude::{block::Block, Testnet3,transactions::Transactions},
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Instant,
    },
    colored::*,

};

fn main() {
    let start = Instant::now();
    type CurrentNetwork = Testnet3;
    const TEST_BASE_URL: &str = "https://s3.us-west-1.amazonaws.com/testnet3.blocks/phase3";
    let blocks = Arc::new(RwLock::new(Vec::new()));
    let blocks_clone = blocks.clone();
    let process = move |block: Block<CurrentNetwork>| {
        blocks_clone.write().push(block);
        Ok(())
    };
    let expected = 50;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let completed_height = load_blocks(TEST_BASE_URL, 0, Some(50), process).await.unwrap();
        println!("Blocks loaded in {} seconds", start.elapsed().as_secs().to_string().red());
        assert_eq!(blocks.read().len(), expected);
        if expected > 0 {
            assert_eq!(blocks.read().last().unwrap().height(), completed_height);
        }
        // Check they are sequential.
        for (i, block) in blocks.read().iter().enumerate() {
            assert_eq!(block.height(), 0 + i as u32);
            println!("block height: {}", block.metadata());
            println!("block fee: {:?}", block.transaction_fee_amounts().into_iter().collect::<Vec<_>>());
            for tx in <Transactions<Testnet3> as Clone>::clone(&block.transactions()).into_iter() {
                println!("transaction base fee: {:?}", tx.base_fee_amount());
                println!("transaction priority fee: {:?}",tx.priority_fee_amount());
                println!("transaction base fee: {:?}",tx.base_fee_amount());
                for ts in tx.transitions().into_iter() {
                    println!("transaction serial number: {:?}", ts.inputs().into_iter().collect::<Vec<_>>());
                    println!("transaction commitment: {:?}", ts.outputs());
                }
            }
        }
    });
    println!("Elapsed: {} seconds", start.elapsed().as_secs())
}
