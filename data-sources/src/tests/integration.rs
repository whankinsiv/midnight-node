use std::env;

use crate::tests::{
	authority_selection::test_authority_selection_match,
	cnight_observation::test_cnight_observation_match,
	federated_authority::test_federated_authority_match, mc_hash::test_mc_hash_match,
	sidechain_rpc::test_sidechain_rpc_match,
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

	test_cnight_observation_match(&postgres_uri, &grpc_endpoint).await.unwrap();
	test_authority_selection_match(&postgres_uri, &grpc_endpoint).await.unwrap();
	test_federated_authority_match(&postgres_uri, &grpc_endpoint).await.unwrap();
	test_mc_hash_match(&postgres_uri, &grpc_endpoint).await.unwrap();
	test_sidechain_rpc_match(&postgres_uri, &grpc_endpoint).await.unwrap();
}
