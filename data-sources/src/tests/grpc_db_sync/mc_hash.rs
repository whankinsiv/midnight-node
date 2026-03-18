use std::{error::Error, sync::Arc};

use midnight_primitives_mainchain_follower::partner_chains_db_sync_data_sources::{
	BlockDataSourceImpl, DbSyncBlockDataSourceConfig, McHashDataSourceImpl,
};
use sidechain_domain::{McBlockHash, mainchain_epoch::MainchainEpochConfig};
use sidechain_mc_hash::McHashDataSource;
use sp_core::offchain::Duration;
use sp_timestamp::Timestamp;

use crate::{
	McHashDataSourceGrpcImpl,
	tests::common::{STANDARD_POOL_CFG, get_connection},
};

const DEFAULT_ACTIVE_SLOTS_COEFF: f64 = 0.05;
const DEFAULT_FIRST_EPOCH_NUMBER: u32 = 0;
const DEFAULT_FIRST_SLOT_NUMBER: u64 = 0;
const DEFAULT_EPOCH_DURATION_MILLIS: Duration = Duration::from_millis(86400000);
const DEFAULT_FIRST_EPOCH_TIMESTAMP_MILLIS: u64 = 1666656000000;
const DEFAULT_SLOT_DURATION_MILLIS: Duration = Duration::from_millis(1000);
const DEFAULT_SECURITY_PARAMETER: u32 = 432;
const DEFAULT_BLOCK_STABILITY_MARGIN: u32 = 0;

const DEFAULT_BLOCK_HASH: McBlockHash = McBlockHash([0; 32]);
const DEFAULT_BLOCK_TIMESTAMP: Timestamp = Timestamp::new(0);

pub async fn test_grpc_mc_hash_grpc_against_db_sync(
	postgres_uri: &str,
	grpc_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let block_source_config = load_block_source_config();

	let db_sync = create_dbsync_mc_hash_source(postgres_uri, block_source_config.clone()).await?;
	let grpc = McHashDataSourceGrpcImpl::connect(&grpc_endpoint, block_source_config).await?;

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

fn load_epoch_config() -> MainchainEpochConfig {
	MainchainEpochConfig {
		epoch_duration_millis: DEFAULT_EPOCH_DURATION_MILLIS,
		slot_duration_millis: DEFAULT_SLOT_DURATION_MILLIS,
		first_epoch_timestamp_millis: DEFAULT_FIRST_EPOCH_TIMESTAMP_MILLIS.into(),
		first_epoch_number: DEFAULT_FIRST_EPOCH_NUMBER,
		first_slot_number: DEFAULT_FIRST_SLOT_NUMBER,
	}
}

fn load_block_source_config() -> DbSyncBlockDataSourceConfig {
	DbSyncBlockDataSourceConfig {
		cardano_security_parameter: DEFAULT_SECURITY_PARAMETER,
		cardano_active_slots_coeff: DEFAULT_ACTIVE_SLOTS_COEFF,
		block_stability_margin: DEFAULT_BLOCK_STABILITY_MARGIN,
	}
}

async fn create_dbsync_mc_hash_source(
	connection_string: &str,
	block_source_config: DbSyncBlockDataSourceConfig,
) -> Result<McHashDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let mc_hash_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	let mc_hash_block_data_source =
		BlockDataSourceImpl::from_config(mc_hash_pool, block_source_config, &load_epoch_config());
	Ok(McHashDataSourceImpl::new(Arc::new(mc_hash_block_data_source), None))
}
