use std::error::Error;

use midnight_primitives_federated_authority_observation::{
	AuthBodyConfig, FederatedAuthorityObservationConfig,
};
use midnight_primitives_mainchain_follower::{
	FederatedAuthorityObservationDataSource, FederatedAuthorityObservationDataSourceImpl,
};
use sidechain_domain::{McBlockHash, PolicyId};

use crate::{
	FederatedAuthorityObservationGrpcImpl,
	tests::common::{STANDARD_POOL_CFG, get_connection},
};

const DEFAULT_BLOCK_HASH: McBlockHash = McBlockHash([0; 32]);
const DEFAULT_COUNCIL_MEMBERS_MAINCHAIN: [PolicyId; 3] = [
	PolicyId([
		0xe3, 0xea, 0xcc, 0x2b, 0xa7, 0xa0, 0xff, 0x8a, 0xe8, 0xd5, 0x28, 0x7a, 0x8e, 0x27, 0x5b,
		0xeb, 0x1b, 0x7d, 0x1e, 0x5f, 0x6f, 0x22, 0x39, 0x4c, 0x44, 0x45, 0xb0, 0x82,
	]),
	PolicyId([
		0xa5, 0xc6, 0xdf, 0x40, 0x8a, 0xbd, 0xbc, 0x52, 0x2a, 0x67, 0xcc, 0x97, 0x6e, 0x17, 0xb4,
		0x4a, 0xa8, 0xe2, 0xef, 0x93, 0x88, 0xc0, 0xe5, 0x88, 0x46, 0xc0, 0xee, 0xa4,
	]),
	PolicyId([
		0x1c, 0xac, 0xdd, 0x48, 0xfb, 0x7e, 0x72, 0x84, 0xca, 0x65, 0x44, 0x65, 0xfa, 0x78, 0xa2,
		0xc2, 0xb2, 0xc1, 0xc0, 0x66, 0x28, 0x5d, 0x51, 0x5e, 0x3a, 0x80, 0x47, 0x2d,
	]),
];
const DEFAULT_TECHNICAL_COMMITEE_MEMBERS_MAINCHAIN: [PolicyId; 3] = [
	PolicyId([
		0xb9, 0x4a, 0x81, 0x87, 0x1d, 0xa1, 0x64, 0x63, 0x7b, 0x21, 0x30, 0xe0, 0x64, 0x34, 0xc3,
		0x00, 0xba, 0x2d, 0x30, 0x88, 0x26, 0x8f, 0x80, 0x98, 0xa7, 0xdd, 0xf2, 0x46,
	]),
	PolicyId([
		0xde, 0x14, 0xef, 0x01, 0x85, 0x4d, 0x8f, 0x22, 0x04, 0xe0, 0xaf, 0x3c, 0x97, 0xbd, 0x1a,
		0xce, 0x07, 0x92, 0x50, 0x8d, 0x39, 0x7c, 0x95, 0x7b, 0x01, 0x0b, 0x1b, 0x70,
	]),
	PolicyId([
		0x11, 0x91, 0xed, 0xfe, 0x59, 0x02, 0x63, 0xa7, 0x76, 0x1b, 0xe9, 0x2d, 0x7e, 0x3d, 0x3d,
		0x82, 0x48, 0xf2, 0x8a, 0x50, 0xd4, 0x10, 0x34, 0x05, 0xfe, 0xf0, 0x1a, 0xe6,
	]),
];

const DEFAULT_COUNCIL_ADDRESS: &str =
	"addr_test1wz28tz6e9zdpvr6f7qsxmyyw5zq0520h334087984ttskpcnczun7";
const DEFAULT_COUNCIL_POLICY_ID: PolicyId = PolicyId([
	0x94, 0x75, 0x8b, 0x59, 0x28, 0x9a, 0x16, 0x0f, 0x49, 0xf0, 0x20, 0x6d, 0x90, 0x8e, 0xa0, 0x80,
	0xfa, 0x29, 0xf7, 0x8c, 0x6a, 0xf3, 0xf8, 0xa7, 0xaa, 0xd7, 0x0b, 0x07,
]);
const DEFAULT_TECHNICAL_COMMITEE_ADDRESS: &str =
	"addr_test1wpj3t7yvs489kjgzlayd37ft6wg9eu6vguem8jgdgu2kengayhe62";
const DEFAULT_TECHNICAL_COMMITEE_POLICY_ID: PolicyId = PolicyId([
	0x65, 0x15, 0xf8, 0x8c, 0x85, 0x4e, 0x5b, 0x49, 0x02, 0xff, 0x48, 0xd8, 0xf9, 0x2b, 0xd3, 0x90,
	0x5c, 0xf3, 0x4c, 0x47, 0x33, 0xb3, 0xc9, 0x0d, 0x47, 0x15, 0x6c, 0xcd,
]);

pub async fn test_grpc_federated_authority_against_db_sync(
	postgres_uri: &str,
	grpc_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
	let config = load_federated_authority_config();

	let db_sync = create_dbsync_federated_authority_source(postgres_uri)
		.await
		.expect("db-sync init failed");
	let grpc = FederatedAuthorityObservationGrpcImpl::connect(&grpc_endpoint)
		.await
		.expect("grpc init failed");

	let db_federated_data = db_sync
		.get_federated_authority_data(&config, &DEFAULT_BLOCK_HASH)
		.await
		.expect("failed to get db-sync federated authority data");
	let grpc_federated_data = grpc
		.get_federated_authority_data(&config, &DEFAULT_BLOCK_HASH)
		.await
		.expect("failed to get grpc federated authority data");

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

fn load_federated_authority_config() -> FederatedAuthorityObservationConfig {
	FederatedAuthorityObservationConfig {
		council: AuthBodyConfig {
			address: DEFAULT_COUNCIL_ADDRESS.to_string(),
			policy_id: DEFAULT_COUNCIL_POLICY_ID,
			members: Vec::new(),
			members_mainchain: DEFAULT_COUNCIL_MEMBERS_MAINCHAIN.to_vec(),
		},
		technical_committee: AuthBodyConfig {
			address: DEFAULT_TECHNICAL_COMMITEE_ADDRESS.to_string(),
			policy_id: DEFAULT_TECHNICAL_COMMITEE_POLICY_ID,
			members: Vec::new(),
			members_mainchain: DEFAULT_TECHNICAL_COMMITEE_MEMBERS_MAINCHAIN.to_vec(),
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
