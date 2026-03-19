use sidechain_mc_hash::McHashDataSource;
use sp_timestamp::Timestamp;

use crate::{
	McHashDataSourceGrpcImpl,
	tests::configuration::{IntegrationTestConfig, ParamsConfig},
};

pub mod authority_selection;
pub mod cnight_observation;
pub mod federated_authority;
pub mod mc_hash;
pub mod sidechain_rpc;

pub async fn load_params_from_grpc(
	config: &IntegrationTestConfig,
) -> Result<ParamsConfig, Box<dyn std::error::Error + Send + Sync>> {
	let grpc = McHashDataSourceGrpcImpl::connect(
		&config.grpc_endpoint,
		config.block_source_config.clone(),
	)
	.await?;

	let stable_block = grpc
		.get_latest_stable_block_for(Timestamp::default())
		.await?
		.ok_or("No stable block returned")?;

	Ok(ParamsConfig {
		epoch_number: stable_block.epoch,
		tx_capacity: 200,
		tip: stable_block.hash,
		timestamp: Timestamp::new(stable_block.timestamp),
	})
}
