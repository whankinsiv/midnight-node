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
	tests::common::{CNIGHT_OBSERVATION_POOL_CFG, get_connection},
};

const DEFAULT_MAPPING_VALIDATOR_ADDRESS: &str =
	"addr_test1wpztklvv6scgyzne56ky0va0x5dmje56lf39eshxdha68rclu8fje";
const DEFAULT_AUTH_TOKEN_ASSET_NAME: &str = "";
const DEFAULT_CNIGHT_POLICY_ID: &str = "03cf16101d110dcad9cacb225f0d1e63a8809979e7feb60426995414";
const DEFAULT_CNIGHT_ASSET_NAME: &str = "";

const DEFAULT_TX_CAPACITY: usize = 200;

pub async fn test_cnight_observation_match(
	postgres_uri: &str,
	grpc_endpoint: &String,
) -> Result<(), Box<dyn std::error::Error>> {
	let config = load_cnight_config().expect("failed to load cNIGHT config");
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

	let db = create_dbsync_cnight_observation_source(postgres_uri)
		.await
		.expect("db-sync init failed");
	let grpc = MidnightCNightObservationGrpcImpl::connect(&grpc_endpoint)
		.await
		.expect("grpc init failed");
	let current_tip = McBlockHash(
		hex::decode("38d7fd275538e995454888c58137fd39cbf454bb2736feb2d81021964029cb93")
			.expect("invalid hex")
			.try_into()
			.expect("wrong length"),
	);

	let db_utxos = db
		.get_utxos_up_to_capacity(&config, &start_position, current_tip.clone(), tx_capacity)
		.await
		.expect("failed to get db-sync utxos");
	let grpc_utxos = grpc
		.get_utxos_up_to_capacity(&config, &start_position, current_tip, tx_capacity)
		.await
		.expect("failed to get grpc utxos");

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

fn load_cnight_config() -> Result<CNightAddresses, Box<dyn Error + Send + Sync>> {
	let cnight_policy_id = hex::decode(
		env::var("CNIGHT_TEST_CNIGHT_POLICY_ID")
			.unwrap_or_else(|_| DEFAULT_CNIGHT_POLICY_ID.to_string()),
	)?
	.try_into()
	.map_err(|_| {
		std::io::Error::new(std::io::ErrorKind::InvalidInput, "wrong cNIGHT policy id length")
	})?;

	Ok(CNightAddresses {
		mapping_validator_address: env::var("CNIGHT_TEST_MAPPING_VALIDATOR_ADDRESS")
			.unwrap_or_else(|_| DEFAULT_MAPPING_VALIDATOR_ADDRESS.to_string()),
		auth_token_asset_name: env::var("CNIGHT_TEST_AUTH_TOKEN_ASSET_NAME")
			.unwrap_or_else(|_| DEFAULT_AUTH_TOKEN_ASSET_NAME.to_string()),
		cnight_policy_id,
		cnight_asset_name: env::var("CNIGHT_TEST_CNIGHT_ASSET_NAME")
			.unwrap_or_else(|_| DEFAULT_CNIGHT_ASSET_NAME.to_string()),
	})
}

async fn create_dbsync_cnight_observation_source(
	connection_string: &str,
) -> Result<MidnightCNightObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let cnight_observation_pool =
		get_connection(connection_string, CNIGHT_OBSERVATION_POOL_CFG, true).await?;
	Ok(MidnightCNightObservationDataSourceImpl::new(cnight_observation_pool, None, 1000))
}
