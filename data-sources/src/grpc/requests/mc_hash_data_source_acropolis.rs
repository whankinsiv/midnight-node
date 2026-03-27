use sidechain_domain::{MainchainBlock, McBlockHash, McBlockNumber, McEpochNumber, McSlotNumber};
use tonic::{Status, transport::Channel};

use crate::grpc::conversions::hash32;
use crate::grpc::midnight_state::midnight_state_client::MidnightStateClient;
use crate::grpc::midnight_state::{
	BlockByHashRequest, LatestStableBlockRequest, StableBlockRequest,
};

pub(crate) async fn get_latest_stable_block(
	client: &mut MidnightStateClient<Channel>,
	stability_offset: u32,
	as_of_timestamp_unix_millis: u64,
) -> Result<Option<MainchainBlock>, Status> {
	let response = client
		.get_latest_stable_block(LatestStableBlockRequest {
			stability_offset,
			as_of_timestamp_unix_millis,
		})
		.await?
		.into_inner();

	response
		.block
		.map(|block| {
			Ok(MainchainBlock {
				number: McBlockNumber(block.block_number),
				hash: McBlockHash(hash32(block.block_hash)?),
				epoch: McEpochNumber(block.epoch_number),
				slot: McSlotNumber(block.slot_number),
				timestamp: block.block_timestamp_unix,
			})
		})
		.transpose()
}
pub(crate) async fn get_stable_block(
	client: &mut MidnightStateClient<Channel>,
	hash: McBlockHash,
	stability_offset: u32,
	as_of_timestamp_unix_millis: u64,
) -> Result<Option<MainchainBlock>, Status> {
	let response = client
		.get_stable_block(StableBlockRequest {
			block_hash: hash.0.to_vec(),
			stability_offset,
			as_of_timestamp_unix_millis,
		})
		.await?
		.into_inner();

	response
		.block
		.map(|block| {
			Ok(MainchainBlock {
				number: McBlockNumber(block.block_number),
				hash: McBlockHash(hash32(block.block_hash)?),
				epoch: McEpochNumber(block.epoch_number),
				slot: McSlotNumber(block.slot_number),
				timestamp: block.block_timestamp_unix,
			})
		})
		.transpose()
}

pub(crate) async fn get_block_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<Option<MainchainBlock>, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(Some(MainchainBlock {
		number: McBlockNumber(response.block_number),
		hash: block_hash,
		epoch: McEpochNumber(response.epoch_number),
		slot: McSlotNumber(response.slot_number),
		timestamp: response.block_timestamp_unix as u64,
	}))
}
