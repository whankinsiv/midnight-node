use std::{error::Error, sync::Arc};

use midnight_primitives_mainchain_follower::partner_chains_db_sync_data_sources::{
	BlockDataSourceImpl, McHashDataSourceImpl,
};
use sidechain_mc_hash::McHashDataSource;

use crate::{
	McHashDataSourceGrpcImpl,
	tests::{
		common::{STANDARD_POOL_CFG, get_connection},
		configuration::{IntegrationTestConfig, ParamsConfig},
	},
};

pub async fn test_grpc_mc_hash_grpc_against_db_sync(
	config: &IntegrationTestConfig,
	params: &ParamsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_sync = create_dbsync_mc_hash_source(config).await?;

	let grpc = McHashDataSourceGrpcImpl::connect(
		&config.grpc_endpoint,
		config.block_source_config.clone(),
	)
	.await?;

	test_block_by_hash_match(&db_sync, &grpc, params).await?;
	test_get_stable_block_from_timestamp(&db_sync, &grpc, params).await?;
	test_get_stable_block_from_hash(&db_sync, &grpc, params).await?;

	Ok(())
}

async fn test_block_by_hash_match(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
	params: &ParamsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	assert_eq!(
		db_sync.get_block_by_hash(params.tip.clone()).await?,
		grpc.get_block_by_hash(params.tip.clone()).await?,
		"block by hash mismatch"
	);

	Ok(())
}

async fn test_get_stable_block_from_timestamp(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
	params: &ParamsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	assert_eq!(
		db_sync.get_latest_stable_block_for(params.timestamp).await?,
		grpc.get_latest_stable_block_for(params.timestamp).await?,
		"block by timestamp mismatch"
	);

	Ok(())
}

async fn test_get_stable_block_from_hash(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
	params: &ParamsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	assert_eq!(
		db_sync.get_stable_block_for(params.tip.clone(), params.timestamp).await?,
		grpc.get_stable_block_for(params.tip.clone(), params.timestamp).await?,
		"stable block by hash mismatch"
	);

	Ok(())
}

async fn create_dbsync_mc_hash_source(
	config: &IntegrationTestConfig,
) -> Result<McHashDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	Ok(McHashDataSourceImpl::new(
		Arc::new(BlockDataSourceImpl::from_config(
			get_connection(&config.postgres_uri, STANDARD_POOL_CFG, true).await?,
			config.block_source_config.clone(),
			&config.epoch_config,
		)),
		None,
	))
}
