#[cfg(test)]
mod tests {
	const CNIGHT_OBSERVATION_POOL_CFG: DbPoolCfg =
		DbPoolCfg { acquire_timeout: std::time::Duration::from_secs(30), max_connections: 10 };

	const POSTGRES_URI: &str = "postgres://postgres:8a91505e310244ba@localhost:15432/cexplorer";
	const GRPC_ENDPOINT: &str = "http://127.0.0.1:50051";

	use std::{error::Error, str::FromStr};

	use midnight_primitives_cnight_observation::{
		CNightAddresses, CardanoPosition, TimestampUnixMillis,
	};
	use midnight_primitives_mainchain_follower::{
		MidnightCNightObservationDataSource, MidnightCNightObservationDataSourceImpl,
	};
	use sidechain_domain::McBlockHash;

	use crate::MidnightCNightObservationGrpcImpl;

	#[tokio::test]
	async fn test_db_sync_against_grpc() {
		let config = CNightAddresses {
			mapping_validator_address:
				"addr_test1wpztklvv6scgyzne56ky0va0x5dmje56lf39eshxdha68rclu8fje".to_string(),
			auth_token_asset_name: "".to_string(),
			cnight_policy_id: hex::decode(
				"03cf16101d110dcad9cacb225f0d1e63a8809979e7feb60426995414",
			)
			.expect("invalid hex")
			.try_into()
			.expect("wrong length"),
			cnight_asset_name: "03cf16101d110dcad9cacb225f0d1e63a8809979e7feb60426995414"
				.to_string(),
		};

		let start_position = CardanoPosition {
			block_hash: McBlockHash(
				hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
					.expect("invalid hex")
					.try_into()
					.expect("wrong length"),
			),
			block_number: 0,
			block_timestamp: TimestampUnixMillis(0),
			tx_index_in_block: 0,
		};

		let current_tip = McBlockHash(
			hex::decode("38d7fd275538e995454888c58137fd39cbf454bb2736feb2d81021964029cb93")
				.expect("invalid hex")
				.try_into()
				.expect("wrong length"),
		);

		// Create sources
		let db = create_dbsync_cnight_observation_source().await.expect("db-sync init failed");

		let grpc = MidnightCNightObservationGrpcImpl::connect(GRPC_ENDPOINT)
			.await
			.expect("grpc init failed");

		// Make the same request to both implementations
		let db_utxos = db
			.get_utxos_up_to_capacity(&config, &start_position, current_tip.clone(), 200)
			.await
			.expect("Failed to get db utxos");

		let grpc_utxos = grpc
			.get_utxos_up_to_capacity(&config, &start_position, current_tip, 200)
			.await
			.expect("Failed to get grpc utxos");

		if db_utxos.utxos.len() != grpc_utxos.utxos.len() {
			panic!("Length mismatch: db={} grpc={}", db_utxos.utxos.len(), grpc_utxos.utxos.len());
		}

		// Deep comparison with useful diff
		for (i, (a, b)) in db_utxos.utxos.iter().zip(grpc_utxos.utxos.iter()).enumerate() {
			if a != b {
				panic!(
					"Mismatch at index {}\n\
                    db:   {}.{}\n\
                    grpc: {}.{}\n\
                    db_full={:#?}\n\
                    grpc_full={:#?}",
					i,
					a.header.tx_position.block_number,
					a.header.tx_position.tx_index_in_block,
					b.header.tx_position.block_number,
					b.header.tx_position.tx_index_in_block,
					a,
					b
				);
			}
		}
	}

	async fn create_dbsync_cnight_observation_source()
	-> Result<MidnightCNightObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
		let cnight_observation_pool =
			get_connection(POSTGRES_URI, CNIGHT_OBSERVATION_POOL_CFG, true).await?;
		Ok(MidnightCNightObservationDataSourceImpl::new(cnight_observation_pool, None, 1000))
	}

	// Copied from internal utility in partner-chains-db-sync-data-sources
	async fn get_connection(
		connection_string: &str,
		pool_cfg: DbPoolCfg,
		allow_non_ssl: bool,
	) -> Result<sqlx::PgPool, Box<dyn Error + Send + Sync + 'static>> {
		let connect_options = sqlx::postgres::PgConnectOptions::from_str(connection_string)?
			.ssl_mode(if allow_non_ssl {
				//Note: PgSslMode::Prefer has issues with some environments.
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

	#[derive(Clone)]
	pub struct DbPoolCfg {
		acquire_timeout: std::time::Duration,
		max_connections: u32,
	}

	#[derive(Debug, Clone, thiserror::Error)]
	#[error("Could not connect to database: postgres://***:***@{0}:{1}/{2}; error: {3}")]
	struct PostgresConnectionError(String, u16, String, String);
}
