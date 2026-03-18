use std::{error::Error, str::FromStr};

use authority_selection_inherents::AuthoritySelectionDataSource;
use midnight_primitives_mainchain_follower::CandidatesDataSourceImpl;
use sidechain_domain::{MainchainAddress, McEpochNumber, PolicyId};

use crate::{
	AuthoritySelectionDataSourceGrpcImpl,
	tests::common::{STANDARD_POOL_CFG, get_connection},
};

const DEFAULT_EPOCH: McEpochNumber = McEpochNumber(600);

const DEFAULT_D_PARAM_POLICY: PolicyId = PolicyId([
	0x11, 0x8a, 0x79, 0xbb, 0xb3, 0xef, 0x8f, 0x72, 0x3e, 0xb0, 0x41, 0x4d, 0xc9, 0x0f, 0x53, 0xb4,
	0xfe, 0xa4, 0xed, 0x8b, 0xdd, 0x60, 0xe5, 0xac, 0x5c, 0x10, 0xd2, 0x70,
]);
const DEFAULT_PERMISSIONED_CANDIDATES_POLICY: PolicyId = PolicyId([
	0x8a, 0xe1, 0x35, 0xf2, 0x79, 0xda, 0x14, 0x07, 0x6c, 0xf1, 0xdf, 0x73, 0xfb, 0x38, 0xdf, 0x70,
	0x7f, 0xbe, 0x21, 0x43, 0xcc, 0xfe, 0x05, 0x05, 0x7c, 0xa4, 0xc0, 0x30,
]);
const DEFAULT_COMMITTEE_CANDIDATE_ADDRESS: &str =
	"addr_test1wre2lz556a58uz3wy9jk2auahurs5cus2vfj3lpszknr4fsx9l69g";

pub async fn test_grpc_authority_selection_against_db_sync(
	postgres_uri: &str,
	grpc_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
	let db_sync = create_dbsync_authority_selection_source(postgres_uri)
		.await
		.expect("db-sync init failed");
	let grpc = AuthoritySelectionDataSourceGrpcImpl::connect(&grpc_endpoint)
		.await
		.expect("grpc init failed");

	test_parameters_match(&grpc, &db_sync).await?;
	test_epoch_candidates_match(&grpc, &db_sync).await?;
	test_epoch_nonce_match(&grpc, &db_sync).await?;
	test_data_epoch_match(&grpc, &db_sync).await?;
	Ok(())
}

async fn test_parameters_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
) -> Result<(), Box<dyn std::error::Error>> {
	let db_params = db_sync
		.get_ariadne_parameters(
			DEFAULT_EPOCH,
			DEFAULT_D_PARAM_POLICY,
			DEFAULT_PERMISSIONED_CANDIDATES_POLICY,
		)
		.await
		.expect("failed to get db-sync parameters");
	let grpc_params = grpc
		.get_ariadne_parameters(
			DEFAULT_EPOCH,
			DEFAULT_D_PARAM_POLICY,
			DEFAULT_PERMISSIONED_CANDIDATES_POLICY,
		)
		.await
		.expect("failed to get grpc parameters");

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
) -> Result<(), Box<dyn std::error::Error>> {
	let committee_candidate_address =
		MainchainAddress::from_str(DEFAULT_COMMITTEE_CANDIDATE_ADDRESS)?;
	let db_candidates = db_sync
		.get_candidates(DEFAULT_EPOCH, committee_candidate_address.clone())
		.await
		.expect("failed to get db-sync epoch candidates");
	let grpc_candidates = grpc
		.get_candidates(DEFAULT_EPOCH, committee_candidate_address)
		.await
		.expect("failed to get grpc epoch candidates");

	assert_eq!(db_candidates, grpc_candidates, "epoch candidates mismatch");
	Ok(())
}

async fn test_epoch_nonce_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
) -> Result<(), Box<dyn std::error::Error>> {
	let db_nonce = db_sync
		.get_epoch_nonce(DEFAULT_EPOCH)
		.await
		.expect("failed to get db-sync epoch nonce");
	let grpc_nonce = grpc
		.get_epoch_nonce(DEFAULT_EPOCH)
		.await
		.expect("failed to get grpc epoch nonce");

	assert_eq!(db_nonce, grpc_nonce, "epoch nonce mismatch");
	Ok(())
}

async fn test_data_epoch_match(
	grpc: &AuthoritySelectionDataSourceGrpcImpl,
	db_sync: &CandidatesDataSourceImpl,
) -> Result<(), Box<dyn std::error::Error>> {
	let db_data_epoch = db_sync
		.data_epoch(DEFAULT_EPOCH)
		.await
		.expect("failed to get db-sync data epoch");
	let grpc_data_epoch =
		grpc.data_epoch(DEFAULT_EPOCH).await.expect("failed to get grpc data epoch");

	assert_eq!(db_data_epoch, grpc_data_epoch, "data epoch mismatch");
	Ok(())
}

async fn create_dbsync_authority_selection_source(
	connection_string: &str,
) -> Result<CandidatesDataSourceImpl, Box<dyn Error + Send + Sync + 'static>> {
	let candidates_pool = get_connection(connection_string, STANDARD_POOL_CFG, true).await?;
	CandidatesDataSourceImpl::new(candidates_pool, None).await
}
