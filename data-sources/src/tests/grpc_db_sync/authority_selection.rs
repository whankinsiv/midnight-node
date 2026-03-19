use std::error::Error;

use authority_selection_inherents::AuthoritySelectionDataSource;
use midnight_primitives_mainchain_follower::CandidatesDataSourceImpl;
use sidechain_domain::McEpochNumber;

use crate::{
	AuthoritySelectionDataSourceGrpcImpl,
	tests::{
		common::{STANDARD_POOL_CFG, get_connection},
		configuration::IntegrationTestConfig,
	},
};

const DEFAULT_EPOCH: McEpochNumber = McEpochNumber(600);

pub async fn test_grpc_authority_selection_against_db_sync(
	config: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_sync = create_dbsync_authority_selection_source(&config.postgres_uri).await?;
	let grpc = AuthoritySelectionDataSourceGrpcImpl::connect(&config.grpc_endpoint).await?;

	test_parameters_match(&grpc, &db_sync, config).await?;
	test_epoch_candidates_match(&grpc, &db_sync, config).await?;
	test_epoch_nonce_match(&grpc, &db_sync).await?;
	test_data_epoch_match(&grpc, &db_sync).await?;
	Ok(())
}

async fn test_parameters_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
	cfg: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_params = db_sync
		.get_ariadne_parameters(
			DEFAULT_EPOCH,
			cfg.d_parameter_policy_id.clone(),
			cfg.permissioned_candidates_policy.clone(),
		)
		.await?;
	let grpc_params = grpc
		.get_ariadne_parameters(
			DEFAULT_EPOCH,
			cfg.d_parameter_policy_id.clone(),
			cfg.permissioned_candidates_policy.clone(),
		)
		.await?;

	assert_eq!(db_params.d_parameter, grpc_params.d_parameter, "d_parameter mismatch");
	assert_eq!(
		db_params.permissioned_candidates, grpc_params.permissioned_candidates,
		"permissioned_candidates mismatch"
	);

	Ok(())
}

async fn test_epoch_candidates_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
	cfg: &IntegrationTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_candidates = db_sync
		.get_candidates(DEFAULT_EPOCH, cfg.committee_candidate_address.clone())
		.await?;
	let grpc_candidates = grpc
		.get_candidates(DEFAULT_EPOCH, cfg.committee_candidate_address.clone())
		.await?;

	assert_eq!(db_candidates, grpc_candidates, "epoch candidates mismatch");
	Ok(())
}

async fn test_epoch_nonce_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_nonce = db_sync.get_epoch_nonce(DEFAULT_EPOCH).await?;
	let grpc_nonce = grpc.get_epoch_nonce(DEFAULT_EPOCH).await?;

	assert_eq!(db_nonce, grpc_nonce, "epoch nonce mismatch");
	Ok(())
}

async fn test_data_epoch_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let db_data_epoch = db_sync.data_epoch(DEFAULT_EPOCH).await?;
	let grpc_data_epoch = grpc.data_epoch(DEFAULT_EPOCH).await?;

	assert_eq!(db_data_epoch, grpc_data_epoch, "data epoch mismatch");
	Ok(())
}

async fn create_dbsync_authority_selection_source(
	connection_string: &str,
) -> Result<CandidatesDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let candidates_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	CandidatesDataSourceImpl::new(candidates_pool, None).await
}
