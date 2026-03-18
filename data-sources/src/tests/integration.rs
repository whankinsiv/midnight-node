use std::env;

use crate::tests::grpc_db_sync::{
	authority_selection::test_grpc_authority_selection_against_db_sync,
	cnight_observation::test_grpc_cnight_observation_against_db_sync,
	federated_authority::test_grpc_federated_authority_against_db_sync,
	mc_hash::test_grpc_mc_hash_grpc_against_db_sync,
	sidechain_rpc::test_grpc_sidechain_rpc_against_db_sync,
};

const DEFAULT_POSTGRES_URI: &str = "postgres://postgres:8a91505e310244ba@localhost:15432/cexplorer";
const DEFAULT_GRPC_ENDPOINT: &str = "http://127.0.0.1:50051";

#[tokio::test]
#[ignore = "requires local db-sync postgres and Acropolis gRPC"]
async fn test_grpc_datasources_against_db_sync() {
	let postgres_uri =
		env::var("CNIGHT_TEST_POSTGRES_URI").unwrap_or_else(|_| DEFAULT_POSTGRES_URI.to_string());
	let grpc_endpoint =
		env::var("CNIGHT_TEST_GRPC_ENDPOINT").unwrap_or_else(|_| DEFAULT_GRPC_ENDPOINT.to_string());

	test_grpc_cnight_observation_against_db_sync(&postgres_uri, &grpc_endpoint)
		.await
		.unwrap();
	test_grpc_authority_selection_against_db_sync(&postgres_uri, &grpc_endpoint)
		.await
		.unwrap();
	test_grpc_federated_authority_against_db_sync(&postgres_uri, &grpc_endpoint)
		.await
		.unwrap();
	test_grpc_mc_hash_grpc_against_db_sync(&postgres_uri, &grpc_endpoint)
		.await
		.unwrap();
	test_grpc_sidechain_rpc_against_db_sync(&postgres_uri, &grpc_endpoint)
		.await
		.unwrap();
}
