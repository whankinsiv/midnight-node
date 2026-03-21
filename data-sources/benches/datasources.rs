use core::fmt;
use std::{env, error::Error, str::FromStr, time::Duration};

use authority_selection_inherents::AuthoritySelectionDataSource;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use midnight_node_data_sources::{
	AuthoritySelectionDataSourceGrpcImpl, MidnightCNightObservationGrpcImpl,
};
use midnight_primitives_cnight_observation::{
	CNightAddresses, CardanoPosition, TimestampUnixMillis,
};
use midnight_primitives_mainchain_follower::{
	CandidatesDataSourceImpl, MidnightCNightObservationDataSource,
	MidnightCNightObservationDataSourceImpl,
};
use sidechain_domain::{McBlockHash, McEpochNumber, PolicyId};

const GRPC_CONNECTION_STRING: &str = "http://127.0.0.1:50051";
const DB_SYNC_ENDPOINT: &str = "postgres://postgres:8a91505e310244ba@localhost:15432/cexplorer";

const TEST_EPOCH: McEpochNumber = McEpochNumber(1220u32);
const TEST_D_PARAM_POLICY: PolicyId = PolicyId([
	0x11, 0x8a, 0x79, 0xbb, 0xb3, 0xef, 0x8f, 0x72, 0x3e, 0xb0, 0x41, 0x4d, 0xc9, 0x0f, 0x53, 0xb4,
	0xfe, 0xa4, 0xed, 0x8b, 0xdd, 0x60, 0xe5, 0xac, 0x5c, 0x10, 0xd2, 0x70,
]);
const PERMISSIONED_CANDIDATES_POLICY: PolicyId = PolicyId([
	0x8a, 0xe1, 0x35, 0xf2, 0x79, 0xda, 0x14, 0x07, 0x6c, 0xf1, 0xdf, 0x73, 0xfb, 0x38, 0xdf, 0x70,
	0x7f, 0xbe, 0x21, 0x43, 0xcc, 0xfe, 0x05, 0x05, 0x7c, 0xa4, 0xc0, 0x30,
]);

fn bench_cnight_utxos(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	//
	// MidnightCNightObservationDataSource
	//

	let (grpc, db_sync) = rt.block_on(async {
		let db = create_dbsync_cnight_observation_source(&bench_postgres_uri())
			.await
			.expect("db init failed");

		let grpc = MidnightCNightObservationGrpcImpl::connect(bench_grpc_endpoint())
			.await
			.expect("grpc connect failed");

		(grpc, db)
	});

	let capacity = 200;

	// ---- get_utxos_up_to_capacity ----
	let mut group = c.benchmark_group("get_utxos_up_to_capacity");
	group.sample_size(10);

	group.bench_function("grpc", |b| {
		let grpc = grpc.clone();
		b.to_async(&rt).iter(|| {
			let grpc = grpc.clone();
			async move {
				grpc.get_utxos_up_to_capacity(
					&cnight_config(),
					&start_position(),
					current_tip(),
					capacity,
				)
				.await
				.unwrap();
			}
		});
	});

	group.bench_function("dbsync", |b| {
		let db_sync = db_sync.clone();
		b.to_async(&rt).iter(|| {
			let db_sync = db_sync.clone();
			async move {
				db_sync
					.get_utxos_up_to_capacity(
						&cnight_config(),
						&start_position(),
						current_tip(),
						capacity,
					)
					.await
					.unwrap();
			}
		});
	});

	group.finish();

	//
	// AuthoritySelectionDataSource
	//

	let (grpc, db_sync) = rt.block_on(async {
		let db = create_dbsync_authority_selection_source(DB_SYNC_ENDPOINT)
			.await
			.expect("db init failed");

		let grpc = AuthoritySelectionDataSourceGrpcImpl::connect(GRPC_CONNECTION_STRING)
			.await
			.expect("grpc connect failed");

		(grpc, db)
	});

	// ---- get_ariadne_parameters ----

	bench_pair(
		c,
		&rt,
		"get_ariadne_parameters",
		{
			let grpc = grpc.clone();
			move || {
				let grpc = grpc.clone();
				async move {
					grpc.get_ariadne_parameters(
						TEST_EPOCH,
						TEST_D_PARAM_POLICY,
						PERMISSIONED_CANDIDATES_POLICY,
					)
					.await
					.unwrap();
				}
			}
		},
		{
			let db_sync = db_sync.clone();
			move || {
				let db_sync = db_sync.clone();
				async move {
					db_sync
						.get_ariadne_parameters(
							TEST_EPOCH,
							TEST_D_PARAM_POLICY,
							PERMISSIONED_CANDIDATES_POLICY,
						)
						.await
						.unwrap();
				}
			}
		},
	);

	// ---- get_candidates ----

	// ---- get_epoch_nonce ----

	// ---- data_epoch ----

	//
	// FederatedAuthorityObservationDataSource
	//

	// ---- get_federated_authority_data ----

	//
	// McHashDataSource
	//

	// ---- get_latest_stable_block_for ----

	// ---- get_stable_block_for ----

	// ---- get_block_by_hash ----

	//
	// SidechainRpcDataSource
	//

	// ---- get_latest_block_info ----
}

criterion_group!(benches, bench_cnight_utxos);
criterion_main!(benches);

// ---- helpers ----
pub const CNIGHT_OBSERVATION_POOL_CFG: DbPoolCfg =
	DbPoolCfg { acquire_timeout: Duration::from_secs(30), max_connections: 10 };

pub const STANDARD_POOL_CFG: DbPoolCfg =
	DbPoolCfg { acquire_timeout: Duration::from_secs(30), max_connections: 5 };

fn bench_pair<F1, F2, Fut1, Fut2>(
	c: &mut Criterion,
	rt: &tokio::runtime::Runtime,
	group_name: &str,
	grpc_fn: F1,
	db_fn: F2,
) where
	F1: FnMut() -> Fut1 + Clone + 'static,
	F2: FnMut() -> Fut2 + Clone + 'static,
	Fut1: std::future::Future<Output = ()> + 'static,
	Fut2: std::future::Future<Output = ()> + 'static,
{
	let mut group = c.benchmark_group(group_name);
	group.sample_size(10);

	group.bench_function("grpc", |b| {
		let mut f = grpc_fn.clone();
		b.to_async(rt).iter(&mut f);
	});

	group.bench_function("dbsync", |b| {
		let mut f = db_fn.clone();
		b.to_async(rt).iter(&mut f);
	});

	group.finish();
}

fn cnight_config() -> CNightAddresses {
	CNightAddresses {
		mapping_validator_address:
			"addr_test1wpztklvv6scgyzne56ky0va0x5dmje56lf39eshxdha68rclu8fje".to_string(),
		auth_token_asset_name: "".to_string(),
		cnight_policy_id: hex::decode("03cf16101d110dcad9cacb225f0d1e63a8809979e7feb60426995414")
			.unwrap()
			.try_into()
			.unwrap(),
		cnight_asset_name: "".to_string(),
	}
}

fn start_position() -> CardanoPosition {
	CardanoPosition {
		block_hash: McBlockHash([0; 32]),
		block_number: 0,
		block_timestamp: TimestampUnixMillis(0),
		tx_index_in_block: 0,
	}
}

fn current_tip() -> McBlockHash {
	McBlockHash(
		hex::decode("38d7fd275538e995454888c58137fd39cbf454bb2736feb2d81021964029cb93")
			.unwrap()
			.try_into()
			.unwrap(),
	)
}

fn bench_postgres_uri() -> String {
	env::var("BENCH_POSTGRES_URI")
		.or_else(|_| env::var("CNIGHT_TEST_POSTGRES_URI"))
		.or_else(|_| env::var("POSTGRES_URI"))
		.unwrap_or_else(|_| "postgres://postgres:8a91505e310244ba@localhost:15432/cexplorer".into())
}

fn bench_grpc_endpoint() -> String {
	env::var("BENCH_GRPC_ENDPOINT")
		.or_else(|_| env::var("GRPC_ENDPOINT"))
		.unwrap_or_else(|_| "http://127.0.0.1:50051".into())
}

async fn create_dbsync_cnight_observation_source(
	connection_string: &str,
) -> Result<MidnightCNightObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	Ok(MidnightCNightObservationDataSourceImpl::new(
		get_connection(connection_string, CNIGHT_OBSERVATION_POOL_CFG, true).await?,
		None,
		1000,
	))
}

async fn create_dbsync_authority_selection_source(
	connection_string: &str,
) -> Result<CandidatesDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	CandidatesDataSourceImpl::new(
		get_connection(connection_string, STANDARD_POOL_CFG, true).await?,
		None,
	)
	.await
}

pub async fn get_connection(
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

#[derive(Debug, Clone, thiserror::Error)]
#[error("Could not connect to database: postgres://***:***@{0}:{1}/{2}; error: {3}")]
pub struct PostgresConnectionError(String, u16, String, String);

#[derive(Clone, Copy)]
pub struct DbPoolCfg {
	acquire_timeout: Duration,
	max_connections: u32,
}

impl fmt::Debug for DbPoolCfg {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DbPoolCfg")
			.field("acquire_timeout", &self.acquire_timeout)
			.field("max_connections", &self.max_connections)
			.finish()
	}
}
