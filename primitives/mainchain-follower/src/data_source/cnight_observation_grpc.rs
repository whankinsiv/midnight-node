use std::cmp::min;

use cardano_serialization_lib::Address;
use midnight_primitives_cnight_observation::{CNightAddresses, CardanoPosition, ObservedUtxos};
use sidechain_domain::McBlockHash;
use tonic::transport::{Channel, Endpoint};

use crate::{
	MidnightCNightObservationDataSource,
	data_source::MidnightCNightObservationDataSourceError,
	grpc::requests::cnight_observation_acropolis::{
		get_asset_creates, get_asset_spends, get_block_number_by_hash, get_deregistrations,
		get_registrations,
	},
	midnight_state::midnight_state_client::MidnightStateClient,
};

// Paginate mainchain queries in fixed block windows to avoid large gRPC responses.
// The window size may need adjustment depending on chain activity and payload size.
const BLOCK_WINDOW: u32 = 1000;
pub struct MidnightCNightObservationGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl MidnightCNightObservationGrpcImpl {
	pub async fn connect(
		endpoint: impl AsRef<str>,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		let endpoint_str = endpoint.as_ref().to_string();

		let endpoint = Endpoint::from_shared(endpoint_str.clone())
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
		_capacity: usize,
	) -> Result<ObservedUtxos, Box<dyn std::error::Error + Send + Sync>> {
		let mapping_validator_address = Address::from_bech32(&config.mapping_validator_address)
			.map_err(|e| {
				MidnightCNightObservationDataSourceError::MappingValidatorInvalidAddress(
					e.to_string(),
				)
			})?;

		let cardano_network = mapping_validator_address.network_id().map_err(|_| {
			MidnightCNightObservationDataSourceError::CardanoNetworkError(
				config.mapping_validator_address.clone(),
			)
		})?;

		let mut client = self.client.clone();

		let tip_block_number =
			get_block_number_by_hash(&mut client, current_tip.clone()).await.map_err(|_| {
				MidnightCNightObservationDataSourceError::MissingBlockReference(current_tip)
			})?;

		let start_block = start_position.block_number;
		let end_block = min(start_block + BLOCK_WINDOW, tip_block_number);

		let mut utxos = Vec::new();
		utxos.extend(get_asset_creates(&mut client, start_block, end_block).await?);
		utxos.extend(get_asset_spends(&mut client, start_block, end_block).await?);
		utxos
			.extend(get_registrations(&mut client, cardano_network, start_block, end_block).await?);
		utxos.extend(
			get_deregistrations(&mut client, cardano_network, start_block, end_block).await?,
		);
		utxos.sort();

		Ok(ObservedUtxos { start: start_position.clone(), end: start_position.clone(), utxos })
	}
}
