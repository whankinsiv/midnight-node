use cardano_serialization_lib::Address;
use midnight_primitives_cnight_observation::{CNightAddresses, CardanoPosition, ObservedUtxos};
use midnight_primitives_mainchain_follower::MidnightCNightObservationDataSource;
use sidechain_domain::McBlockHash;
use tonic::transport::{Channel, Endpoint};

use crate::{
	grpc::{
		midnight_state::midnight_state_client::MidnightStateClient,
		requests::cnight_observation_acropolis::get_utxo_events,
	},
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

#[derive(Clone)]
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

		let response =
			get_utxo_events(&mut client, cardano_network, start_position, current_tip, tx_capacity)
				.await
				.map_err(AcropolisDataSourceError::GRPCQueryError)?;

		let start = start_position.clone();
		let end = response.next_position;
		let utxos = response.events;

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
