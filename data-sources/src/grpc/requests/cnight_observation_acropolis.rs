use crate::grpc::conversions::observed_utxo_from_event;
use crate::grpc::midnight_state::{
	CardanoPosition as CardanoPositionProto, UtxoEventsRequest,
	midnight_state_client::MidnightStateClient,
};
use midnight_primitives_cnight_observation::{CardanoPosition, ObservedUtxo, TimestampUnixMillis};
use sidechain_domain::*;
use tonic::Status;
use tonic::transport::Channel;

pub struct UtxoEventsResult {
	pub events: Vec<ObservedUtxo>,
	pub next_position: CardanoPosition,
}

pub async fn get_utxo_events(
	client: &mut MidnightStateClient<Channel>,
	cardano_network: u8,
	start_position: &CardanoPosition,
	end_block_hash: McBlockHash,
	tx_capacity: usize,
) -> Result<UtxoEventsResult, Status> {
	let tx_capacity = u32::try_from(tx_capacity)
		.map_err(|_| tonic::Status::invalid_argument("utxo_capacity too large"))?;

	let response = client
		.get_utxo_events(UtxoEventsRequest {
			start_block: start_position.block_number,
			start_tx_index: start_position.tx_index_in_block,
			tx_capacity,
			end_block_hash: end_block_hash.0.to_vec(),
			start_position: Some(CardanoPositionProto {
				block_hash: start_position.block_hash.0.to_vec(),
				block_number: start_position.block_number,
				tx_index: start_position.tx_index_in_block,
				block_timestamp_unix_millis: start_position.block_timestamp.0,
			}),
		})
		.await?
		.into_inner();

	let events = response
		.events
		.into_iter()
		.map(|e| observed_utxo_from_event(e, cardano_network))
		.collect::<Result<Vec<_>, _>>()?;

	let next_position = response
		.next_position
		.ok_or_else(|| tonic::Status::internal("missing next_position"))?;
	let next_block_hash: [u8; 32] = next_position
		.block_hash
		.try_into()
		.map_err(|_| Status::internal("invalid hash length"))?;

	Ok(UtxoEventsResult {
		events,
		next_position: CardanoPosition {
			block_hash: McBlockHash(next_block_hash),
			block_number: next_position.block_number,
			block_timestamp: TimestampUnixMillis(next_position.block_timestamp_unix_millis),
			tx_index_in_block: next_position.tx_index,
		},
	})
}
