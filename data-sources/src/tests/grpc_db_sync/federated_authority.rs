use std::error::Error;

use midnight_primitives_federated_authority_observation::{
	AuthBodyConfig, FederatedAuthorityObservationConfig,
};
use midnight_primitives_mainchain_follower::{
	FederatedAuthorityObservationDataSource, FederatedAuthorityObservationDataSourceImpl,
};
use sidechain_domain::McBlockHash;

use crate::{
	FederatedAuthorityObservationGrpcImpl,
	tests::{
		common::{STANDARD_POOL_CFG, get_connection},
		configuration::IntegrationTestConfig,
	},
};

const DEFAULT_BLOCK_HASH: McBlockHash = McBlockHash([0; 32]);

pub async fn test_grpc_federated_authority_against_db_sync(
	test_config: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let config = load_federated_authority_config(test_config.clone());

	let db_sync = create_dbsync_federated_authority_source(&test_config.postgres_uri).await?;
	let grpc = FederatedAuthorityObservationGrpcImpl::connect(&test_config.grpc_endpoint).await?;

	let db_federated_data =
		db_sync.get_federated_authority_data(&config, &DEFAULT_BLOCK_HASH).await?;
	let grpc_federated_data =
		grpc.get_federated_authority_data(&config, &DEFAULT_BLOCK_HASH).await?;

	assert_eq!(
		db_federated_data.council_authorities, grpc_federated_data.council_authorities,
		"council authorities mismatch"
	);
	assert_eq!(
		db_federated_data.technical_committee_authorities,
		grpc_federated_data.technical_committee_authorities,
		"technical committee authorities mismatch"
	);
	assert_eq!(
		db_federated_data.mc_block_hash, grpc_federated_data.mc_block_hash,
		"federated data block hash mismatch"
	);

	Ok(())
}

fn load_federated_authority_config(
	cfg: IntegrationTestConfig,
) -> FederatedAuthorityObservationConfig {
	FederatedAuthorityObservationConfig {
		council: AuthBodyConfig {
			address: cfg.council_address,
			policy_id: cfg.council_policy_id,
			members: Vec::new(),
			members_mainchain: cfg.council_members_mainchain,
		},
		technical_committee: AuthBodyConfig {
			address: cfg.technical_commitee_address,
			policy_id: cfg.technical_commitee_policy_id,
			members: Vec::new(),
			members_mainchain: cfg.technical_commitee_members_mainchain,
		},
	}
}

async fn create_dbsync_federated_authority_source(
	connection_string: &str,
) -> Result<FederatedAuthorityObservationDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let federated_authority_observation_pool =
		get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	Ok(FederatedAuthorityObservationDataSourceImpl::new(
		federated_authority_observation_pool,
		None,
		1000,
	))
}
