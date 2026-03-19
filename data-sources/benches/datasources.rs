use core::fmt;
use std::{error::Error, str::FromStr, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use midnight_node_data_sources::MidnightCNightObservationGrpcImpl;
use midnight_primitives_cnight_observation::{
	CNightAddresses, CardanoPosition, TimestampUnixMillis,
};
use midnight_primitives_mainchain_follower::{
	MidnightCNightObservationDataSource, MidnightCNightObservationDataSourceImpl,
};
use sidechain_domain::McBlockHash;

pub const CNIGHT_OBSERVATION_POOL_CFG: DbPoolCfg =
	DbPoolCfg { acquire_timeout: Duration::from_secs(30), max_connections: 10 };

fn bench_cnight_utxos(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	let (grpc, db_sync) = rt.block_on(async {
		let db = create_dbsync_cnight_observation_source(
			"postgres://postgres:8a91505e310244ba@localhost:15432/cexplorer",
		)
		.await
		.expect("db init failed");

		let grpc = MidnightCNightObservationGrpcImpl::connect("http://127.0.0.1:50051")
			.await
			.expect("grpc connect failed");

		(grpc, db)
	});

	let capacity = 200;

	//
	// MidnightCNightObservationDataSource
	//

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

	// ---- get_ariadne_parameters ----

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

async fn create_dbsync_cnight_observation_source(
	connection_string: &str,
) -> Result<MidnightCNightObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	Ok(MidnightCNightObservationDataSourceImpl::new(
		get_connection(connection_string, CNIGHT_OBSERVATION_POOL_CFG, true).await?,
		None,
		1000,
	))
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
