use cardano_serialization_lib::Address;
use midnight_primitives_cnight_observation::{
	CNightAddresses, CardanoPosition, ObservedUtxo, ObservedUtxos,
};
use midnight_primitives_mainchain_follower::MidnightCNightObservationDataSource;
use sidechain_domain::McBlockHash;
use tonic::transport::{Channel, Endpoint};

use crate::{
	grpc::{midnight_state::midnight_state_client::MidnightStateClient, requests::cnight_observation_acropolis::{get_position_by_hash, get_utxo_events}},
	sources::AcropolisDataSourceError,
};

#[derive(thiserror::Error, Debug)]
pub enum AcropolisCNightObservationDataSourceError {
	#[error("Error extracting network id from Cardano address")]
	CardanoNetworkError(String),
	#[error("Invalid value for mapping validator address")]
	MappingValidatorInvalidAddress(String),
	#[error("Error querying gRPC `{0}`")]
	GRPCQueryError(tonic::Status),
}

pub struct MidnightCNightObservationGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl MidnightCNightObservationGrpcImpl {
	pub async fn connect(
		endpoint: impl AsRef<str>,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		let endpoint_str = endpoint.as_ref();

		let endpoint = Endpoint::from_shared(endpoint_str.to_string())
			.map_err(|e| format!("Invalid gRPC endpoint `{}`: {}", endpoint_str, e))?
			.tcp_nodelay(true)
			.http2_keep_alive_interval(std::time::Duration::from_secs(30))
			.keep_alive_while_idle(true);

		let channel = endpoint.connect().await.map_err(|e| {
			format!("Failed to connect to gRPC server at `{}`: {}", endpoint_str, e)
		})?;

		Ok(Self { client: MidnightStateClient::new(channel) })
	}
}

#[async_trait::async_trait]
impl MidnightCNightObservationDataSource for MidnightCNightObservationGrpcImpl {
	async fn get_utxos_up_to_capacity(
		&self,
		config: &CNightAddresses,
		start_position: &CardanoPosition,
		current_tip: McBlockHash,
		tx_capacity: usize,
	) -> Result<ObservedUtxos, Box<dyn std::error::Error + Send + Sync>> {
		let cardano_network = get_cardano_network(config)?;

		let mut client = self.client.clone();

		let utxos = get_utxo_events(
			&mut client,
			cardano_network,
			start_position.block_number,
			start_position.tx_index_in_block,
			tx_capacity,
		)
		.await
		.map_err(AcropolisDataSourceError::GRPCQueryError)?;

		// tx_count is intentionally incremented by one to match the db-sync impl
		let tx_count = count_distinct_transactions(&utxos) + 1;

		let start = start_position.clone();
		let end = if tx_count < tx_capacity {
			let end = get_position_by_hash(&mut client, current_tip.clone())
				.await
				.map_err(|_| AcropolisDataSourceError::MissingBlockReference(current_tip))?;
			end.increment()
		} else {
			utxos.last().map_or(start.clone(), |u| u.header.tx_position.clone()).increment()
		};

		Ok(ObservedUtxos { start, end, utxos })
	}
}

#[allow(clippy::result_large_err)]
fn get_cardano_network(
	config: &CNightAddresses,
) -> Result<u8, AcropolisCNightObservationDataSourceError> {
	let addr = Address::from_bech32(&config.mapping_validator_address).map_err(|e| {
		AcropolisCNightObservationDataSourceError::MappingValidatorInvalidAddress(e.to_string())
	})?;

	addr.network_id().map_err(|_| {
		AcropolisCNightObservationDataSourceError::CardanoNetworkError(
			config.mapping_validator_address.clone(),
		)
	})
}

fn count_distinct_transactions(utxos: &[ObservedUtxo]) -> usize {
	let mut tx_count = 0usize;
	let mut last_tx: Option<(u32, u32)> = None;

	for u in utxos {
		let pos = &u.header.tx_position;
		let cur = (pos.block_number, pos.tx_index_in_block);

		if last_tx.is_none_or(|prev| prev < cur) {
			tx_count += 1;
			last_tx = Some(cur);
		}
	}

	tx_count
}
