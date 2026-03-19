use std::{env, error::Error};

use midnight_primitives_cnight_observation::{
	CNightAddresses, CardanoPosition, TimestampUnixMillis,
};
use midnight_primitives_mainchain_follower::{
	MidnightCNightObservationDataSource, MidnightCNightObservationDataSourceImpl,
};
use sidechain_domain::McBlockHash;

use crate::{
	MidnightCNightObservationGrpcImpl,
	tests::{
		common::{CNIGHT_OBSERVATION_POOL_CFG, get_connection},
		configuration::IntegrationTestConfig,
	},
};

const DEFAULT_TX_CAPACITY: usize = 200;

pub async fn test_grpc_cnight_observation_against_db_sync(
	test_config: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let config = load_cnight_config(test_config.clone())?;
	let start_position = CardanoPosition {
		block_hash: McBlockHash([0; 32]),
		block_number: 0,
		block_timestamp: TimestampUnixMillis(0),
		tx_index_in_block: 0,
	};
	let tx_capacity = env::var("CNIGHT_TEST_TX_CAPACITY")
		.ok()
		.and_then(|value| value.parse::<usize>().ok())
		.unwrap_or(DEFAULT_TX_CAPACITY);

	let db = create_dbsync_cnight_observation_source(&test_config.postgres_uri).await?;
	let grpc =
		MidnightCNightObservationGrpcImpl::connect(test_config.grpc_endpoint.clone()).await?;

	let tip_raw = hex::decode("38d7fd275538e995454888c58137fd39cbf454bb2736feb2d81021964029cb93")?;
	let tip_bytes: [u8; 32] = tip_raw
		.as_slice()
		.try_into()
		.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
	let current_tip = McBlockHash(tip_bytes);

	let db_utxos = db
		.get_utxos_up_to_capacity(&config, &start_position, current_tip.clone(), tx_capacity)
		.await?;
	let grpc_utxos = grpc
		.get_utxos_up_to_capacity(&config, &start_position, current_tip, tx_capacity)
		.await?;

	assert_eq!(db_utxos.start, grpc_utxos.start, "start_position mismatch");

	assert_eq!(db_utxos.utxos.len(), grpc_utxos.utxos.len(), "UTxO length mismatch");

	for (i, (db, grpc)) in db_utxos.utxos.iter().zip(&grpc_utxos.utxos).enumerate() {
		if db != grpc {
			panic!("UTxO mismatch at index {i}");
		}
	}

	assert_eq!(db_utxos.end, grpc_utxos.end, "end_position mismatch");

	Ok(())
}

fn load_cnight_config(
	cfg: IntegrationTestConfig,
) -> Result<CNightAddresses, Box<dyn Error + Send + Sync>> {
	Ok(CNightAddresses {
		mapping_validator_address: cfg.mapping_validator_address,
		auth_token_asset_name: cfg.auth_token_asset_name,
		cnight_policy_id: cfg.cnight_policy_id,
		cnight_asset_name: cfg.cnight_asset_name,
	})
}

async fn create_dbsync_cnight_observation_source(
	connection_string: &str,
) -> Result<MidnightCNightObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let cnight_observation_pool =
		get_connection(connection_string, CNIGHT_OBSERVATION_POOL_CFG, true).await?;
	Ok(MidnightCNightObservationDataSourceImpl::new(cnight_observation_pool, None, 1000))
}
