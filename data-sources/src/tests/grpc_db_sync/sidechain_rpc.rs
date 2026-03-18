use std::{error::Error, sync::Arc};

use midnight_primitives_mainchain_follower::partner_chains_db_sync_data_sources::{
	BlockDataSourceImpl, DbSyncBlockDataSourceConfig, SidechainRpcDataSourceImpl,
};
use pallet_sidechain_rpc::SidechainRpcDataSource;
use sidechain_domain::mainchain_epoch::MainchainEpochConfig;
use sp_core::offchain::Duration;

use crate::{
	SidechainRpcDataSourceGrpcImpl,
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

pub async fn test_grpc_sidechain_rpc_against_db_sync(
	postgres_uri: &str,
	grpc_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_sync = create_dbsync_sidechain_rpc_source(postgres_uri).await?;
	let grpc = SidechainRpcDataSourceGrpcImpl::connect(grpc_endpoint).await?;

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

async fn create_dbsync_sidechain_rpc_source(
	connection_string: &str,
) -> Result<SidechainRpcDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let sidechain_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	let sidechain_block_data_source = Arc::new(BlockDataSourceImpl::from_config(
		sidechain_pool,
		load_block_source_config(),
		&load_epoch_config(),
	));
	Ok(SidechainRpcDataSourceImpl::new(sidechain_block_data_source.clone(), None))
}
