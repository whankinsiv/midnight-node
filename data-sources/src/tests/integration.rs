use std::{
	collections::{BTreeMap, BTreeSet},
	env,
	error::Error,
	fmt,
	str::FromStr,
	time::{Duration, Instant},
};

use midnight_primitives_cnight_observation::{
	CNightAddresses, CardanoPosition, ObservedUtxo, ObservedUtxoData, TimestampUnixMillis,
};
use midnight_primitives_mainchain_follower::{
	MidnightCNightObservationDataSource, MidnightCNightObservationDataSourceImpl,
};
use sidechain_domain::McBlockHash;
use tonic::Request;

use crate::MidnightCNightObservationGrpcImpl;
use crate::grpc::midnight_state::{LatestBlockRequest, midnight_state_client::MidnightStateClient};

const DEFAULT_POSTGRES_URI: &str = "postgres://postgres:postgres@localhost:5432/cexplorer";
const DEFAULT_GRPC_ENDPOINT: &str = "http://127.0.0.1:65051";
const DEFAULT_MAPPING_VALIDATOR_ADDRESS: &str =
	"addr_test1wpztklvv6scgyzne56ky0va0x5dmje56lf39eshxdha68rclu8fje";
const DEFAULT_AUTH_TOKEN_ASSET_NAME: &str = "";
const DEFAULT_CNIGHT_POLICY_ID: &str = "03cf16101d110dcad9cacb225f0d1e63a8809979e7feb60426995414";
const DEFAULT_CNIGHT_ASSET_NAME: &str = "";
const DEFAULT_TX_CAPACITY: usize = 200;

const CNIGHT_OBSERVATION_POOL_CFG: DbPoolCfg =
	DbPoolCfg { acquire_timeout: Duration::from_secs(30), max_connections: 10 };

#[tokio::test]
#[ignore = "requires local db-sync postgres and Acropolis gRPC"]
async fn test_db_sync_against_grpc() {
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
	let postgres_uri =
		env::var("CNIGHT_TEST_POSTGRES_URI").unwrap_or_else(|_| DEFAULT_POSTGRES_URI.to_string());
	let grpc_endpoint =
		env::var("CNIGHT_TEST_GRPC_ENDPOINT").unwrap_or_else(|_| DEFAULT_GRPC_ENDPOINT.to_string());

	let db = create_dbsync_cnight_observation_source(&postgres_uri)
		.await
		.expect("db-sync init failed");
	let grpc = MidnightCNightObservationGrpcImpl::connect(&grpc_endpoint)
		.await
		.expect("grpc init failed");
	let current_tip = resolve_current_tip(&grpc)
		.await
		.expect("failed to determine current tip from env or grpc");
	wait_for_db_block(&postgres_uri, &current_tip, Duration::from_secs(15))
		.await
		.expect("db-sync never resolved the chosen current tip");

	let db_utxos = db
		.get_utxos_up_to_capacity(&config, &start_position, current_tip.clone(), tx_capacity)
		.await
		.expect("failed to get db-sync utxos");
	let grpc_utxos = grpc
		.get_utxos_up_to_capacity(&config, &start_position, current_tip, tx_capacity)
		.await
		.expect("failed to get grpc utxos");

	if db_utxos.utxos.is_empty() && grpc_utxos.utxos.is_empty() {
		panic!(
			"Both db-sync and gRPC returned zero cNIGHT events.\n{}",
			format_context(&config, &postgres_uri, &grpc_endpoint, tx_capacity),
		);
	}

	if !db_utxos.utxos.is_empty() && grpc_utxos.utxos.is_empty() {
		panic!(
			"gRPC returned zero cNIGHT events while db-sync returned {}.\n\
This usually means the Acropolis instance has not indexed any midnight_state cNIGHT or mapping events.\n\
{}",
			db_utxos.utxos.len(),
			format_context(&config, &postgres_uri, &grpc_endpoint, tx_capacity),
		);
	}

	if db_utxos.utxos.len() != grpc_utxos.utxos.len() {
		panic!(
			"Length mismatch: db={} grpc={}\n{}\n{}",
			db_utxos.utxos.len(),
			grpc_utxos.utxos.len(),
			format_context(&config, &postgres_uri, &grpc_endpoint, tx_capacity),
			format_set_diff(&db_utxos.utxos, &grpc_utxos.utxos),
		);
	}

	for (i, (db, grpc)) in db_utxos.utxos.iter().zip(grpc_utxos.utxos.iter()).enumerate() {
		if db != grpc {
			panic!(
				"Mismatch at index {}\n\
db_key={}\n\
grpc_key={}\n\
db_full={:#?}\n\
grpc_full={:#?}",
				i,
				utxo_key(db),
				utxo_key(grpc),
				db,
				grpc,
			);
		}
	}
}

fn format_context(
	config: &CNightAddresses,
	postgres_uri: &str,
	grpc_endpoint: &str,
	tx_capacity: usize,
) -> String {
	format!(
		"postgres_uri={postgres_uri}\n\
grpc_endpoint={grpc_endpoint}\n\
mapping_validator_address={}\n\
auth_token_asset_name={:?}\n\
cnight_policy_id={}\n\
cnight_asset_name={:?}\n\
tx_capacity={tx_capacity}",
		config.mapping_validator_address,
		config.auth_token_asset_name,
		hex::encode(config.cnight_policy_id),
		config.cnight_asset_name,
	)
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

async fn wait_for_db_block(
	connection_string: &str,
	hash: &McBlockHash,
	timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
	let pool = get_connection(connection_string, CNIGHT_OBSERVATION_POOL_CFG, true).await?;
	let deadline = Instant::now() + timeout;

	loop {
		if db_has_block(&pool, hash).await? {
			return Ok(());
		}

		if Instant::now() >= deadline {
			return Err(
				format!("timed out waiting for db-sync block {}", hex::encode(hash.0)).into()
			);
		}

		tokio::time::sleep(Duration::from_secs(1)).await;
	}
}

async fn db_has_block(pool: &sqlx::PgPool, hash: &McBlockHash) -> Result<bool, sqlx::Error> {
	sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM block WHERE hash = $1)")
		.bind(hash.0.as_slice())
		.fetch_one(pool)
		.await
}

fn format_set_diff(db_utxos: &[ObservedUtxo], grpc_utxos: &[ObservedUtxo]) -> String {
	let db_keys: BTreeSet<String> = db_utxos.iter().map(utxo_key).collect();
	let grpc_keys: BTreeSet<String> = grpc_utxos.iter().map(utxo_key).collect();

	let only_db: Vec<_> = db_keys.difference(&grpc_keys).take(20).cloned().collect();
	let only_grpc: Vec<_> = grpc_keys.difference(&db_keys).take(20).cloned().collect();

	let mut db_kind_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
	let mut grpc_kind_counts: BTreeMap<&'static str, usize> = BTreeMap::new();

	for utxo in db_utxos {
		*db_kind_counts.entry(utxo_kind(utxo)).or_default() += 1;
	}
	for utxo in grpc_utxos {
		*grpc_kind_counts.entry(utxo_kind(utxo)).or_default() += 1;
	}

	format!(
		"db_kind_counts={db_kind_counts:?}\n\
grpc_kind_counts={grpc_kind_counts:?}\n\
only_in_db(first_20)={only_db:#?}\n\
only_in_grpc(first_20)={only_grpc:#?}"
	)
}

fn utxo_kind(utxo: &ObservedUtxo) -> &'static str {
	match &utxo.data {
		ObservedUtxoData::Registration(_) => "registration",
		ObservedUtxoData::Deregistration(_) => "deregistration",
		ObservedUtxoData::AssetCreate(_) => "asset_create",
		ObservedUtxoData::AssetSpend(_) => "asset_spend",
	}
}

fn utxo_key(utxo: &ObservedUtxo) -> String {
	format!(
		"kind={} block={} tx={} tx_hash={} utxo={}#{}",
		utxo_kind(utxo),
		utxo.header.tx_position.block_number,
		utxo.header.tx_position.tx_index_in_block,
		hex::encode(utxo.header.tx_hash.0),
		hex::encode(utxo.header.utxo_tx_hash.0),
		utxo.header.utxo_index.0,
	)
}

fn parse_mc_hash(value: &str) -> Result<McBlockHash, Box<dyn Error + Send + Sync>> {
	Ok(McBlockHash(hex::decode(value)?.try_into().map_err(|_| {
		std::io::Error::new(std::io::ErrorKind::InvalidInput, "wrong mainchain hash length")
	})?))
}

async fn resolve_current_tip(
	grpc: &MidnightCNightObservationGrpcImpl,
) -> Result<McBlockHash, Box<dyn Error + Send + Sync>> {
	if let Ok(value) = env::var("CNIGHT_TEST_CURRENT_TIP") {
		return parse_mc_hash(&value);
	}

	let mut client: MidnightStateClient<_> = grpc.client.clone();
	let response = client.get_latest_block(Request::new(LatestBlockRequest {})).await?;
	let block = response.into_inner().block.ok_or_else(|| {
		std::io::Error::new(std::io::ErrorKind::NotFound, "gRPC latest block response was empty")
	})?;

	Ok(McBlockHash(block.block_hash.try_into().map_err(|_| {
		std::io::Error::new(std::io::ErrorKind::InvalidInput, "wrong latest block hash length")
	})?))
}

async fn get_connection(
	connection_string: &str,
	pool_cfg: DbPoolCfg,
	allow_non_ssl: bool,
) -> Result<sqlx::PgPool, Box<dyn Error + Send + Sync + 'static>> {
	let connect_options =
		sqlx::postgres::PgConnectOptions::from_str(connection_string)?.ssl_mode(if allow_non_ssl {
			sqlx::postgres::PgSslMode::Disable
		} else {
			sqlx::postgres::PgSslMode::Require
		});

	let pool = sqlx::postgres::PgPoolOptions::new()
		.max_connections(pool_cfg.max_connections)
		.acquire_timeout(pool_cfg.acquire_timeout)
		.connect_with(connect_options.clone())
		.await
		.map_err(|e| {
			PostgresConnectionError(
				connect_options.get_host().to_string(),
				connect_options.get_port(),
				connect_options.get_database().unwrap_or("cexplorer").to_string(),
				e.to_string(),
			)
			.to_string()
		})?;
	Ok(pool)
}

#[derive(Clone, Copy)]
struct DbPoolCfg {
	acquire_timeout: Duration,
	max_connections: u32,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Could not connect to database: postgres://***:***@{0}:{1}/{2}; error: {3}")]
struct PostgresConnectionError(String, u16, String, String);

impl fmt::Debug for DbPoolCfg {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DbPoolCfg")
			.field("acquire_timeout", &self.acquire_timeout)
			.field("max_connections", &self.max_connections)
			.finish()
	}
}
