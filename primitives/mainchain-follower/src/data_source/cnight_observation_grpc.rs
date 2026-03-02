use cardano_serialization_lib::Address;
use midnight_primitives_cnight_observation::{CNightAddresses, CardanoPosition, ObservedUtxos};
use sidechain_domain::McBlockHash;
use tonic::transport::Channel;

use crate::{
	MidnightCNightObservationDataSource,
	data_source::MidnightCNightObservationDataSourceError,
	grpc::requests::cnight_observation_acropolis::{
		get_asset_creates, get_asset_spends, get_deregistrations, get_registrations,
	},
	midnight_state::midnight_state_client::MidnightStateClient,
};

// Paginate mainchain queries in fixed block windows to avoid large gRPC responses.
// The window size may need adjustment depending on chain activity and payload size.
const BLOCK_WINDOW: u32 = 1000;
pub struct MidnightCNightObservationGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

#[async_trait::async_trait]
impl MidnightCNightObservationDataSource for MidnightCNightObservationGrpcImpl {
	async fn get_utxos_up_to_capacity(
		&self,
		config: &CNightAddresses,
		start_position: &CardanoPosition,
		_current_tip: McBlockHash,
		_capacity: usize,
	) -> Result<ObservedUtxos, Box<dyn std::error::Error + Send + Sync>> {
		let start_block = start_position.block_number;
		let end_block = start_block + BLOCK_WINDOW;

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

		let creates = get_asset_creates(&mut client, start_block, end_block).await?;
		let spends = get_asset_spends(&mut client, start_block, end_block).await?;
		let registrations =
			get_registrations(&mut client, cardano_network, start_block, end_block).await?;
		let deregistrations =
			get_deregistrations(&mut client, cardano_network, start_block, end_block).await?;

		let mut utxos = Vec::new();
		utxos.extend(creates);
		utxos.extend(spends);
		utxos.extend(registrations);
		utxos.extend(deregistrations);

		Ok(ObservedUtxos { start: start_position.clone(), end: start_position.clone(), utxos })
	}
}
