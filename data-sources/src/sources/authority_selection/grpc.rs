use authority_selection_inherents::{AriadneParameters, AuthoritySelectionDataSource};
use sidechain_domain::{
	CandidateRegistrations, DParameter, EpochNonce, MainchainAddress, McEpochNumber, PolicyId,
	offset_data_epoch,
};
use tonic::transport::{Channel, Endpoint};

use crate::{
	grpc::{
		midnight_state::midnight_state_client::MidnightStateClient,
		requests::authority_selection_data_source_acropolis::{
			get_candidates, get_epoch_nonce, get_permissioned_candidates,
		},
	},
	sources::cnight_observation::grpc::AcropolisCNightObservationDataSourceError,
};

#[derive(Clone)]
pub struct AuthoritySelectionDataSourceGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl AuthoritySelectionDataSourceGrpcImpl {
	pub async fn connect(
		endpoint: impl AsRef<str>,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		let endpoint_str = endpoint.as_ref();

		let endpoint = Endpoint::from_shared(endpoint_str.to_string())
			.map_err(|e| format!("Invalid gRPC endpoint `{}`: {}", endpoint_str, e))?
			.tcp_nodelay(true)
			.http2_keep_alive_interval(std::time::Duration::from_secs(30))
			.keep_alive_while_idle(true);

		let channel = endpoint.connect().await.map_err(|e| {
			format!("Failed to connect to gRPC server at `{}`: {}", endpoint_str, e)
		})?;

		Ok(Self { client: MidnightStateClient::new(channel) })
	}
}

#[async_trait::async_trait]
impl AuthoritySelectionDataSource for AuthoritySelectionDataSourceGrpcImpl {
	async fn get_ariadne_parameters(
		&self,
		epoch: McEpochNumber,
		_d_parameter_policy: PolicyId,
		_permissioned_candidate_policy: PolicyId,
	) -> Result<AriadneParameters, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let query_epoch = authority_selection_query_epoch(epoch)?;

		// DParameter is now read from pallet_system_parameters storage, not from mainchain.
		// This hardcoded value is unused - the actual d_parameter comes from the runtime.
		Ok(AriadneParameters {
			d_parameter: DParameter {
				num_permissioned_candidates: 0,
				num_registered_candidates: 0,
			},
			permissioned_candidates: get_permissioned_candidates(&mut client, query_epoch)
				.await
				.map_err(AcropolisCNightObservationDataSourceError::GRPCQueryError)?,
		})
	}

	async fn get_candidates(
		&self,
		epoch: McEpochNumber,
		_committee_candidate_address: MainchainAddress,
	) -> Result<Vec<CandidateRegistrations>, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let query_epoch = authority_selection_query_epoch(epoch)?;

		get_candidates(&mut client, query_epoch).await.map_err(grpc_err)
	}

	async fn get_epoch_nonce(
		&self,
		epoch: McEpochNumber,
	) -> Result<Option<EpochNonce>, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let query_epoch = authority_selection_query_epoch(epoch)?;

		get_epoch_nonce(&mut client, query_epoch).await.map_err(grpc_err)
	}

	async fn data_epoch(
		&self,
		for_epoch: McEpochNumber,
	) -> Result<McEpochNumber, Box<dyn std::error::Error + Send + Sync>> {
		authority_selection_query_epoch(for_epoch)
	}
}

fn grpc_err(e: tonic::Status) -> Box<dyn std::error::Error + Send + Sync> {
	Box::new(AcropolisCNightObservationDataSourceError::GRPCQueryError(e))
}

fn authority_selection_query_epoch(
	for_epoch: McEpochNumber,
) -> Result<McEpochNumber, Box<dyn std::error::Error + Send + Sync>> {
	offset_data_epoch(&for_epoch).map_err(|offset| {
		format!(
			"Cannot derive authority-selection data epoch for {for_epoch}; expected at least offset {offset}"
		)
		.into()
	})
}
