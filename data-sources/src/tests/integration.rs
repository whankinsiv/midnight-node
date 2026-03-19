use crate::tests::{
	configuration::IntegrationTestConfig,
	grpc_db_sync::{
		authority_selection::test_grpc_authority_selection_against_db_sync,
		cnight_observation::test_grpc_cnight_observation_against_db_sync,
		federated_authority::test_grpc_federated_authority_against_db_sync,
		mc_hash::test_grpc_mc_hash_grpc_against_db_sync,
		sidechain_rpc::test_grpc_sidechain_rpc_against_db_sync,
	},
};

#[tokio::test]
#[ignore = "requires local db-sync postgres and Acropolis gRPC"]
async fn test_grpc_datasources_against_db_sync() {
	let config = IntegrationTestConfig::from_env().expect("failed to load integration test config");

	test_grpc_cnight_observation_against_db_sync(&config)
		.await
		.unwrap_or_else(|e| panic!("cnight observation test failed: {e:?}"));

	test_grpc_authority_selection_against_db_sync(&config)
		.await
		.unwrap_or_else(|e| panic!("authority selection test failed: {e:?}"));

	test_grpc_federated_authority_against_db_sync(&config)
		.await
		.unwrap_or_else(|e| panic!("federated authority test failed: {e:?}"));

	test_grpc_mc_hash_grpc_against_db_sync(&config)
		.await
		.unwrap_or_else(|e| panic!("mc hash test failed: {e:?}"));

	test_grpc_sidechain_rpc_against_db_sync(&config)
		.await
		.unwrap_or_else(|e| panic!("sidechain rpc test failed: {e:?}"));
}
