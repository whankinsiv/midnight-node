pub mod authority_selection;
pub mod bridge;
pub mod cnight_observation;
pub mod federated_authority;
pub mod mc_hash;
pub mod sidechain_rpc;

#[derive(thiserror::Error, Debug)]
pub enum AcropolisDataSourceError {
	#[error("Error querying gRPC `{0}`")]
	GRPCQueryError(tonic::Status),
}
