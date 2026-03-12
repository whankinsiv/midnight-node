use crate::grpc::conversions::observed_utxo_from_event;
use crate::grpc::midnight_state::{
	BlockByHashRequest, UtxoEventsRequest, midnight_state_client::MidnightStateClient,
};
use midnight_primitives_cnight_observation::{CardanoPosition, ObservedUtxo, TimestampUnixMillis};
use sidechain_domain::*;
use tonic::Status;
use tonic::transport::Channel;

pub async fn get_utxo_events(
	client: &mut MidnightStateClient<Channel>,
	cardano_network: u8,
	start_block: u32,
	start_tx_index: u32,
	tx_capacity: usize,
) -> Result<Vec<ObservedUtxo>, Status> {
	let tx_capacity = u32::try_from(tx_capacity)
		.map_err(|_| tonic::Status::invalid_argument("utxo_capacity too large"))?;

	let response = client
		.get_utxo_events(UtxoEventsRequest { start_block, start_tx_index, tx_capacity })
		.await?
		.into_inner();

	response
		.events
		.into_iter()
		.map(|e| observed_utxo_from_event(e, cardano_network))
		.collect()
}

pub(crate) async fn get_position_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<CardanoPosition, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(CardanoPosition {
		block_hash,
		block_number: response.block_number,
		block_timestamp: TimestampUnixMillis(response.block_timestamp_unix * 1000),
		tx_index_in_block: response.tx_count,
	})
}
