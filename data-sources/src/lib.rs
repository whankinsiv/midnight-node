mod grpc;
mod sources;
pub use grpc::client::MidnightGrpcClient;
pub use sources::{
	authority_selection::grpc::AuthoritySelectionDataSourceGrpcImpl,
	cnight_observation::grpc::MidnightCNightObservationGrpcImpl,
	federated_authority::grpc::FederatedAuthorityObservationGrpcImpl,
	mc_hash::grpc::McHashDataSourceGrpcImpl, sidechain_rpc::grpc::SidechainRpcDataSourceGrpcImpl,
};

#[cfg(test)]
mod tests {
	mod integration;

	mod authority_selection;
	mod cnight_observation;
	mod common;
	mod federated_authority;
	mod mc_hash;
	mod sidechain_rpc;
}
