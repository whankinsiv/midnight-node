use std::{error::Error, sync::Arc};

use midnight_primitives_mainchain_follower::partner_chains_db_sync_data_sources::{
	BlockDataSourceImpl, DbSyncBlockDataSourceConfig, McHashDataSourceImpl,
};
use sidechain_domain::{McBlockHash, mainchain_epoch::MainchainEpochConfig};
use sidechain_mc_hash::McHashDataSource;
use sp_timestamp::Timestamp;

use crate::{
	McHashDataSourceGrpcImpl,
	tests::{
		common::{STANDARD_POOL_CFG, get_connection},
		configuration::IntegrationTestConfig,
	},
};

const DEFAULT_BLOCK_HASH: McBlockHash = McBlockHash([0; 32]);
const DEFAULT_BLOCK_TIMESTAMP: Timestamp = Timestamp::new(0);

pub async fn test_grpc_mc_hash_grpc_against_db_sync(
	config: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let block_source_config = config.load_block_source_config();
	let epoch_config = config.load_epoch_config();

	let db_sync = create_dbsync_mc_hash_source(
		&config.postgres_uri,
		block_source_config.clone(),
		&epoch_config,
	)
	.await?;
	let grpc =
		McHashDataSourceGrpcImpl::connect(&config.grpc_endpoint, block_source_config).await?;

	test_block_by_hash_match(&db_sync, &grpc).await?;
	test_get_stable_block_from_timestamp(&db_sync, &grpc).await?;
	test_get_stable_block_from_hash(&db_sync, &grpc).await?;
	Ok(())
}

async fn test_block_by_hash_match(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_block_info = db_sync.get_block_by_hash(DEFAULT_BLOCK_HASH).await?;
	let grpc_block_info = grpc.get_block_by_hash(DEFAULT_BLOCK_HASH).await?;

	assert_eq!(db_block_info, grpc_block_info, "block by hash mismatch");

	Ok(())
}

async fn test_get_stable_block_from_timestamp(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_block_info = db_sync.get_latest_stable_block_for(DEFAULT_BLOCK_TIMESTAMP).await?;
	let grpc_block_info = grpc.get_latest_stable_block_for(DEFAULT_BLOCK_TIMESTAMP).await?;

	assert_eq!(db_block_info, grpc_block_info, "block by timestamp mismatch");

	Ok(())
}

async fn test_get_stable_block_from_hash(
	db_sync: &McHashDataSourceImpl,
	grpc: &McHashDataSourceGrpcImpl,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_block_info = db_sync
		.get_stable_block_for(DEFAULT_BLOCK_HASH, DEFAULT_BLOCK_TIMESTAMP)
		.await?;
	let grpc_block_info =
		grpc.get_stable_block_for(DEFAULT_BLOCK_HASH, DEFAULT_BLOCK_TIMESTAMP).await?;

	assert_eq!(db_block_info, grpc_block_info, "stable block by hash mismatch");

	Ok(())
}

async fn create_dbsync_mc_hash_source(
	connection_string: &str,
	block_source_config: DbSyncBlockDataSourceConfig,
	epoch_config: &MainchainEpochConfig,
) -> Result<McHashDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let mc_hash_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	let mc_hash_block_data_source =
		BlockDataSourceImpl::from_config(mc_hash_pool, block_source_config, epoch_config);
	Ok(McHashDataSourceImpl::new(Arc::new(mc_hash_block_data_source), None))
}
