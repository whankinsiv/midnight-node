mod grpc;
mod sources;
pub use grpc::client::MidnightGrpcClient;
pub use sources::{
	authority_selection::grpc::AuthoritySelectionDataSourceGrpcImpl,
	bridge::grpc::TokenBridgeDataSourceGrpcImpl,
	cnight_observation::grpc::MidnightCNightObservationGrpcImpl,
	federated_authority::grpc::FederatedAuthorityObservationGrpcImpl,
	mc_hash::grpc::McHashDataSourceGrpcImpl, sidechain_rpc::grpc::SidechainRpcDataSourceGrpcImpl,
};

#[cfg(test)]
mod tests;
