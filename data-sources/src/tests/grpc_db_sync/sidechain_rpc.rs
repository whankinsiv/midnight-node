use std::{error::Error, sync::Arc};

use midnight_primitives_mainchain_follower::partner_chains_db_sync_data_sources::{
	BlockDataSourceImpl, SidechainRpcDataSourceImpl,
};
use pallet_sidechain_rpc::SidechainRpcDataSource;

use crate::{
	SidechainRpcDataSourceGrpcImpl,
	tests::{
		common::{STANDARD_POOL_CFG, get_connection},
		configuration::IntegrationTestConfig,
	},
};

pub async fn test_grpc_sidechain_rpc_against_db_sync(
	config: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_sync = create_dbsync_sidechain_rpc_source(&config.postgres_uri, config).await?;
	let grpc = SidechainRpcDataSourceGrpcImpl::connect(&config.grpc_endpoint).await?;

	let db_sync_block_info = db_sync.get_latest_block_info().await?;
	let grpc_block_info = grpc.get_latest_block_info().await?;

	let db_block = db_sync_block_info.number.0;
	let grpc_block = grpc_block_info.number.0;

	let diff = db_block.abs_diff(grpc_block);

	assert!(
		diff <= 50,
		"Block numbers differ too much: db_sync={}, grpc={}, diff={}",
		db_block,
		grpc_block,
		diff
	);

	Ok(())
}

async fn create_dbsync_sidechain_rpc_source(
	connection_string: &str,
	config: &IntegrationTestConfig,
) -> Result<SidechainRpcDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let sidechain_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	let sidechain_block_data_source = Arc::new(BlockDataSourceImpl::from_config(
		sidechain_pool,
		config.block_source_config.clone(),
		&config.epoch_config,
	));
	Ok(SidechainRpcDataSourceImpl::new(sidechain_block_data_source.clone(), None))
}
