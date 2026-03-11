use sidechain_domain::{MainchainBlock, McBlockHash, McBlockNumber, McEpochNumber, McSlotNumber};
use tonic::{Status, transport::Channel};

use crate::midnight_state::LatestBlockRequest;
use crate::{
	grpc::conversions::hash32, midnight_state::midnight_state_client::MidnightStateClient,
};

pub(crate) async fn get_latest_block(
	client: &mut MidnightStateClient<Channel>,
) -> Result<MainchainBlock, Status> {
	let response = client.get_latest_block(LatestBlockRequest {}).await?.into_inner();

	let block = response
		.block
		.ok_or_else(|| Status::internal("LatestBlockResponse missing block"))?;

	Ok(MainchainBlock {
		number: McBlockNumber(block.block_number),
		hash: McBlockHash(hash32(block.block_hash)?),
		epoch: McEpochNumber(block.epoch_number),
		slot: McSlotNumber(block.slot_number),
		timestamp: block.block_timestamp_unix,
	})
}
