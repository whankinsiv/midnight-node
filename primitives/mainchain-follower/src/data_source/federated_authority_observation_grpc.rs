use cardano_serialization_lib::PlutusData;
use midnight_primitives_federated_authority_observation::{
	AuthoritiesData, FederatedAuthorityData, FederatedAuthorityObservationConfig,
};
use sidechain_domain::{McBlockHash, PolicyId};
use tonic::Code;
use tonic::transport::{Channel, Endpoint};

use crate::{
	FederatedAuthorityObservationDataSource,
	data_source::federated_authority_observation::{
		decode_governance_datum, empty_authorities_data,
	},
	grpc::requests::federated_authority_observation_acropolis::{
		get_block_number_by_hash, get_council_datum, get_technical_committee_datum,
	},
	midnight_state::midnight_state_client::MidnightStateClient,
};

pub struct FederatedAuthorityObservationGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl FederatedAuthorityObservationGrpcImpl {
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
impl FederatedAuthorityObservationDataSource for FederatedAuthorityObservationGrpcImpl {
	async fn get_federated_authority_data(
		&self,
		config: &FederatedAuthorityObservationConfig,
		mc_block_hash: &McBlockHash,
	) -> Result<FederatedAuthorityData, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let block_number = get_block_number_by_hash(&mut client, mc_block_hash.clone()).await?;

		let council_authorities = load_authorities(
			get_council_datum(&mut client, block_number).await,
			block_number,
			"council",
			&config.council.address,
			&config.council.policy_id,
		)?;

		let technical_committee_authorities = load_authorities(
			get_technical_committee_datum(&mut client, block_number).await,
			block_number,
			"technical committee",
			&config.technical_committee.address,
			&config.technical_committee.policy_id,
		)?;

		Ok(FederatedAuthorityData {
			council_authorities,
			technical_committee_authorities,
			mc_block_hash: mc_block_hash.clone(),
		})
	}
}

fn load_authorities(
	response: Result<Vec<u8>, tonic::Status>,
	block_number: u32,
	body_name: &str,
	address: &str,
	policy_id: &PolicyId,
) -> Result<AuthoritiesData, Box<dyn std::error::Error + Send + Sync>> {
	match response {
		Ok(bytes) => {
			let authorities = PlutusData::from_bytes(bytes)
				.map_err(|e| format!("Invalid {} datum CBOR: {}", body_name, e))
				.and_then(|datum| {
					decode_governance_datum(&datum)
						.map(AuthoritiesData::from)
						.map_err(|error| error.to_string())
				});

			match authorities {
				Ok(authorities) => Ok(authorities),
				Err(error) => {
					log::warn!(
						"Failed to decode {} datum in Cardano block {}: {}. Using empty list.",
						body_name,
						block_number,
						error,
					);
					Ok(empty_authorities_data())
				},
			}
		},
		Err(status) if status.code() == Code::NotFound => {
			log::warn!(
				"No {} datum found for Cardano block {} (address: {}, policy_id: {}). Using empty list.",
				body_name,
				block_number,
				address,
				policy_id,
			);
			Ok(empty_authorities_data())
		},
		Err(status) => Err(format!(
			"Failed to fetch {} datum for Cardano block {}: {}",
			body_name, block_number, status
		)
		.into()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::midnight_state::{
		AriadneParametersRequest, AriadneParametersResponse, AssetCreatesRequest,
		AssetCreatesResponse, AssetSpendsRequest, AssetSpendsResponse, BlockByHashRequest,
		BlockByHashResponse, CouncilDatumRequest, CouncilDatumResponse, DeregistrationsRequest,
		DeregistrationsResponse, RegistrationsRequest, RegistrationsResponse,
		TechnicalCommitteeDatumRequest, TechnicalCommitteeDatumResponse, UtxoEventsRequest,
		UtxoEventsResponse,
		midnight_state_server::{MidnightState, MidnightStateServer},
	};
	use cardano_serialization_lib::{BigInt, PlutusData, PlutusList, PlutusMap, PlutusMapValues};
	use midnight_primitives_federated_authority_observation::{
		AuthBodyConfig, AuthorityMemberPublicKey,
	};
	use tokio::net::TcpListener;
	use tokio::sync::oneshot;
	use tokio_stream::wrappers::TcpListenerStream;
	use tonic::{Request, Response, Status, transport::Server};

	#[derive(Clone)]
	struct TestMidnightStateService {
		block_by_hash: Result<BlockByHashResponse, Status>,
		council_datum: Result<Option<CouncilDatumResponse>, Status>,
		technical_committee_datum: Result<Option<TechnicalCommitteeDatumResponse>, Status>,
	}

	#[tonic::async_trait]
	impl MidnightState for TestMidnightStateService {
		async fn get_asset_creates(
			&self,
			_request: Request<AssetCreatesRequest>,
		) -> Result<Response<AssetCreatesResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_asset_spends(
			&self,
			_request: Request<AssetSpendsRequest>,
		) -> Result<Response<AssetSpendsResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_registrations(
			&self,
			_request: Request<RegistrationsRequest>,
		) -> Result<Response<RegistrationsResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_deregistrations(
			&self,
			_request: Request<DeregistrationsRequest>,
		) -> Result<Response<DeregistrationsResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_technical_committee_datum(
			&self,
			_request: Request<TechnicalCommitteeDatumRequest>,
		) -> Result<Response<TechnicalCommitteeDatumResponse>, Status> {
			match &self.technical_committee_datum {
				Ok(Some(response)) => Ok(Response::new(response.clone())),
				Ok(None) => Err(Status::not_found("technical committee datum not found")),
				Err(status) => Err(status.clone()),
			}
		}

		async fn get_council_datum(
			&self,
			_request: Request<CouncilDatumRequest>,
		) -> Result<Response<CouncilDatumResponse>, Status> {
			match &self.council_datum {
				Ok(Some(response)) => Ok(Response::new(response.clone())),
				Ok(None) => Err(Status::not_found("council datum not found")),
				Err(status) => Err(status.clone()),
			}
		}

		async fn get_ariadne_parameters(
			&self,
			_request: Request<AriadneParametersRequest>,
		) -> Result<Response<AriadneParametersResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_block_by_hash(
			&self,
			_request: Request<BlockByHashRequest>,
		) -> Result<Response<BlockByHashResponse>, Status> {
			match &self.block_by_hash {
				Ok(response) => Ok(Response::new(*response)),
				Err(status) => Err(status.clone()),
			}
		}

		async fn get_utxo_events(
			&self,
			_request: Request<UtxoEventsRequest>,
		) -> Result<Response<UtxoEventsResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}
	}

	struct TestServer {
		endpoint: String,
		shutdown_tx: Option<oneshot::Sender<()>>,
	}

	impl TestServer {
		async fn spawn(service: TestMidnightStateService) -> Self {
			let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
			let addr = listener.local_addr().unwrap();
			let (shutdown_tx, shutdown_rx) = oneshot::channel();

			tokio::spawn(async move {
				Server::builder()
					.add_service(MidnightStateServer::new(service))
					.serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
						let _ = shutdown_rx.await;
					})
					.await
					.unwrap();
			});

			Self { endpoint: format!("http://{}", addr), shutdown_tx: Some(shutdown_tx) }
		}
	}

	impl Drop for TestServer {
		fn drop(&mut self) {
			if let Some(shutdown_tx) = self.shutdown_tx.take() {
				let _ = shutdown_tx.send(());
			}
		}
	}

	#[tokio::test]
	async fn returns_authorities_from_grpc_datums() {
		let council =
			governance_datum_bytes(2, &[([0xAA; 32], [0x11; 32]), ([0xBB; 32], [0x22; 32])]);
		let technical_committee = governance_datum_bytes(3, &[([0xCC; 32], [0x33; 32])]);
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Ok(BlockByHashResponse {
				block_number: 42,
				tx_count: 7,
				block_timestamp_unix: 1_700_000_000,
			}),
			council_datum: Ok(Some(CouncilDatumResponse {
				source_block_number: 42,
				datum: council,
			})),
			technical_committee_datum: Ok(Some(TechnicalCommitteeDatumResponse {
				source_block_number: 42,
				datum: technical_committee,
			})),
		})
		.await;
		let data_source =
			FederatedAuthorityObservationGrpcImpl::connect(&server.endpoint).await.unwrap();
		let mc_block_hash = McBlockHash([0x44; 32]);

		let actual = data_source
			.get_federated_authority_data(&test_config(), &mc_block_hash)
			.await
			.unwrap();

		assert_eq!(actual.mc_block_hash, mc_block_hash);
		assert_eq!(
			sort_authorities(actual.council_authorities),
			sort_authorities(AuthoritiesData {
				authorities: vec![
					(AuthorityMemberPublicKey(vec![0x11; 32]), PolicyId([0xAA; 28])),
					(AuthorityMemberPublicKey(vec![0x22; 32]), PolicyId([0xBB; 28])),
				],
				round: 2,
			})
		);
		assert_eq!(
			actual.technical_committee_authorities,
			AuthoritiesData {
				authorities: vec![(AuthorityMemberPublicKey(vec![0x33; 32]), PolicyId([0xCC; 28]))],
				round: 3,
			}
		);
	}

	#[tokio::test]
	async fn missing_single_datum_returns_empty_authorities_for_that_body() {
		let technical_committee = governance_datum_bytes(4, &[([0xDD; 32], [0x55; 32])]);
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Ok(BlockByHashResponse {
				block_number: 99,
				tx_count: 1,
				block_timestamp_unix: 1_700_000_001,
			}),
			council_datum: Ok(None),
			technical_committee_datum: Ok(Some(TechnicalCommitteeDatumResponse {
				source_block_number: 99,
				datum: technical_committee,
			})),
		})
		.await;
		let data_source =
			FederatedAuthorityObservationGrpcImpl::connect(&server.endpoint).await.unwrap();

		let actual = data_source
			.get_federated_authority_data(&test_config(), &McBlockHash([0x12; 32]))
			.await
			.unwrap();

		assert_eq!(actual.council_authorities, empty_authorities_data());
		assert_eq!(
			actual.technical_committee_authorities,
			AuthoritiesData {
				authorities: vec![(AuthorityMemberPublicKey(vec![0x55; 32]), PolicyId([0xDD; 28]))],
				round: 4,
			}
		);
	}

	#[tokio::test]
	async fn invalid_datum_bytes_degrade_to_empty_authorities() {
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Ok(BlockByHashResponse {
				block_number: 7,
				tx_count: 2,
				block_timestamp_unix: 1_700_000_002,
			}),
			council_datum: Ok(Some(CouncilDatumResponse {
				source_block_number: 7,
				datum: vec![0xFF, 0x00, 0xAA],
			})),
			technical_committee_datum: Ok(Some(TechnicalCommitteeDatumResponse {
				source_block_number: 7,
				datum: vec![0x01, 0x02, 0x03],
			})),
		})
		.await;
		let data_source =
			FederatedAuthorityObservationGrpcImpl::connect(&server.endpoint).await.unwrap();

		let actual = data_source
			.get_federated_authority_data(&test_config(), &McBlockHash([0x77; 32]))
			.await
			.unwrap();

		assert_eq!(actual.council_authorities, empty_authorities_data());
		assert_eq!(actual.technical_committee_authorities, empty_authorities_data());
	}

	#[tokio::test]
	async fn unknown_block_hash_returns_error() {
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Err(Status::not_found("block not found")),
			council_datum: Ok(None),
			technical_committee_datum: Ok(None),
		})
		.await;
		let data_source =
			FederatedAuthorityObservationGrpcImpl::connect(&server.endpoint).await.unwrap();

		let error = data_source
			.get_federated_authority_data(&test_config(), &McBlockHash([0x99; 32]))
			.await
			.unwrap_err();

		assert!(error.to_string().contains("block not found"));
	}

	fn governance_datum_bytes(round: u8, authorities: &[([u8; 32], [u8; 32])]) -> Vec<u8> {
		let mut members_map = PlutusMap::new();
		for (mainchain_key, authority_key) in authorities {
			let mut values = PlutusMapValues::new();
			values.add(&PlutusData::new_bytes(authority_key.to_vec()));
			members_map.insert(&PlutusData::new_bytes(mainchain_key.to_vec()), &values);
		}

		let mut multisig = PlutusList::new();
		multisig.add(&PlutusData::new_integer(&BigInt::from(authorities.len() as u64)));
		multisig.add(&PlutusData::new_map(&members_map));

		let mut versioned = PlutusList::new();
		versioned.add(&PlutusData::new_list(&multisig));
		versioned.add(&PlutusData::new_integer(&BigInt::from(round)));

		PlutusData::new_list(&versioned).to_bytes()
	}

	fn test_config() -> FederatedAuthorityObservationConfig {
		FederatedAuthorityObservationConfig {
			council: AuthBodyConfig {
				address: "addr_test1council".to_string(),
				policy_id: PolicyId([0x01; 28]),
				members: vec![],
				members_mainchain: vec![],
			},
			technical_committee: AuthBodyConfig {
				address: "addr_test1tech".to_string(),
				policy_id: PolicyId([0x02; 28]),
				members: vec![],
				members_mainchain: vec![],
			},
		}
	}

	fn sort_authorities(mut data: AuthoritiesData) -> AuthoritiesData {
		data.authorities.sort_by(|(left_key, left_member), (right_key, right_member)| {
			left_key.0.cmp(&right_key.0).then(left_member.0.cmp(&right_member.0))
		});
		data
	}
}
