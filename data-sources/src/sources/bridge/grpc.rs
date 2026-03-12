// This file is part of midnight-node.
// Copyright (C) 2025-2026 Midnight Foundation
// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error::Error, fmt::Debug};

use sidechain_domain::McBlockHash;
use sp_partner_chains_bridge::{
	BridgeDataCheckpoint, BridgeTransferV1, MainChainScripts, TokenBridgeDataSource,
};
use tonic::transport::{Channel, Endpoint};

use crate::{
	grpc::{
		midnight_state::midnight_state_client::MidnightStateClient,
		requests::bridge_data_source_acropolis::get_bridge_transfers,
	},
	sources::AcropolisDataSourceError,
};

pub struct TokenBridgeDataSourceGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl TokenBridgeDataSourceGrpcImpl {
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
impl<RecipientAddress> TokenBridgeDataSource<RecipientAddress> for TokenBridgeDataSourceGrpcImpl
where
	RecipientAddress: Debug + for<'a> TryFrom<&'a [u8]> + Send + Sync,
{
	async fn get_transfers(
		&self,
		_main_chain_scripts: MainChainScripts,
		data_checkpoint: BridgeDataCheckpoint,
		max_transfers: u32,
		current_mc_block: McBlockHash,
	) -> Result<
		(Vec<BridgeTransferV1<RecipientAddress>>, BridgeDataCheckpoint),
		Box<dyn Error + Send + Sync>,
	> {
		let mut client = self.client.clone();

		get_bridge_transfers(&mut client, data_checkpoint, max_transfers, current_mc_block)
			.await
			.map_err(grpc_err)
	}
}

fn grpc_err(status: tonic::Status) -> Box<dyn std::error::Error + Send + Sync> {
	Box::new(AcropolisDataSourceError::GRPCQueryError(status))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::grpc::midnight_state::{
		AriadneParametersRequest, AriadneParametersResponse, AssetCreatesRequest,
		AssetCreatesResponse, AssetSpendsRequest, AssetSpendsResponse, BlockByHashRequest,
		BlockByHashResponse, BridgeCheckpoint, BridgeUtxo, BridgeUtxosRequest, BridgeUtxosResponse,
		CouncilDatumRequest, CouncilDatumResponse, DeregistrationsRequest, DeregistrationsResponse,
		EpochCandidatesRequest, EpochCandidatesResponse, EpochNonceRequest, EpochNonceResponse,
		LatestBlockRequest, LatestBlockResponse, LatestStableBlockRequest,
		LatestStableBlockResponse, RegistrationsRequest, RegistrationsResponse, StableBlockRequest,
		StableBlockResponse, TechnicalCommitteeDatumRequest, TechnicalCommitteeDatumResponse,
		UtxoEventsRequest, UtxoEventsResponse, UtxoId, bridge_checkpoint, bridge_utxos_request,
		midnight_state_server::{MidnightState, MidnightStateServer},
	};
	use cardano_serialization_lib::PlutusData;
	use partner_chains_plutus_data::bridge::TokenTransferDatumV1;
	use sidechain_domain::{UtxoId as DomainUtxoId, byte_string::ByteString};
	use tokio::net::TcpListener;
	use tokio::sync::{Mutex, oneshot};
	use tokio_stream::wrappers::TcpListenerStream;
	use tonic::{Request, Response, Status, transport::Server};

	#[derive(Clone)]
	struct TestMidnightStateService {
		block_by_hash: Result<BlockByHashResponse, Status>,
		bridge_utxos: Result<BridgeUtxosResponse, Status>,
		last_request: Arc<Mutex<Option<BridgeUtxosRequest>>>,
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

		async fn get_bridge_utxos(
			&self,
			request: Request<BridgeUtxosRequest>,
		) -> Result<Response<BridgeUtxosResponse>, Status> {
			*self.last_request.lock().await = Some(request.into_inner());
			match &self.bridge_utxos {
				Ok(response) => Ok(Response::new(response.clone())),
				Err(status) => Err(status.clone()),
			}
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
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_council_datum(
			&self,
			_request: Request<CouncilDatumRequest>,
		) -> Result<Response<CouncilDatumResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
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

		async fn get_epoch_nonce(
			&self,
			_request: Request<EpochNonceRequest>,
		) -> Result<Response<EpochNonceResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_epoch_candidates(
			&self,
			_request: Request<EpochCandidatesRequest>,
		) -> Result<Response<EpochCandidatesResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_stable_block(
			&self,
			_request: Request<StableBlockRequest>,
		) -> Result<Response<StableBlockResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_latest_stable_block(
			&self,
			_request: Request<LatestStableBlockRequest>,
		) -> Result<Response<LatestStableBlockResponse>, Status> {
			Err(Status::unimplemented("not used in tests"))
		}

		async fn get_latest_block(
			&self,
			_request: Request<LatestBlockRequest>,
		) -> Result<Response<LatestBlockResponse>, Status> {
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
	async fn decodes_bridge_transfers_and_returns_next_checkpoint() {
		let last_request = Arc::new(Mutex::new(None));
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Ok(BlockByHashResponse {
				block_number: 42,
				tx_count: 0,
				block_timestamp_unix: 0,
				epoch_number: 0,
				slot_number: 0,
			}),
			bridge_utxos: Ok(BridgeUtxosResponse {
				utxos: vec![
					BridgeUtxo {
						tx_hash: vec![0x11; 32],
						output_index: 0,
						block_number: 40,
						block_hash: vec![0x21; 32],
						tx_index: 1,
						block_timestamp_unix: 1_700_000_000,
						tokens_out: 10,
						tokens_in: 3,
						datum: Some(user_transfer_datum_bytes([0xAA; 32])),
					},
					BridgeUtxo {
						tx_hash: vec![0x12; 32],
						output_index: 1,
						block_number: 41,
						block_hash: vec![0x22; 32],
						tx_index: 2,
						block_timestamp_unix: 1_700_000_001,
						tokens_out: 20,
						tokens_in: 5,
						datum: Some(reserve_transfer_datum_bytes()),
					},
				],
				next_checkpoint: Some(BridgeCheckpoint {
					kind: Some(bridge_checkpoint::Kind::Utxo(UtxoId {
						tx_hash: vec![0x12; 32],
						index: 1,
					})),
				}),
			}),
			last_request: last_request.clone(),
		})
		.await;

		let data_source = TokenBridgeDataSourceGrpcImpl::connect(&server.endpoint).await.unwrap();
		let (transfers, checkpoint): (Vec<BridgeTransferV1<[u8; 32]>>, BridgeDataCheckpoint) =
			data_source
				.get_transfers(
					MainChainScripts::default(),
					BridgeDataCheckpoint::Block(sidechain_domain::McBlockNumber(9)),
					10,
					McBlockHash([0x99; 32]),
				)
				.await
				.unwrap();

		assert_eq!(
			transfers,
			vec![
				BridgeTransferV1::UserTransfer { token_amount: 7, recipient: [0xAA; 32] },
				BridgeTransferV1::ReserveTransfer { token_amount: 15 },
			]
		);
		assert_eq!(checkpoint, BridgeDataCheckpoint::Utxo(DomainUtxoId::new([0x12; 32], 1)));

		let request = last_request.lock().await.clone().expect("bridge request should be captured");
		assert_eq!(request.to_block, 42);
		assert_eq!(request.utxo_capacity, 10);
		assert_eq!(request.checkpoint, Some(bridge_utxos_request::Checkpoint::BlockNumber(9)));
	}

	#[tokio::test]
	async fn skips_zero_delta_and_marks_invalid_bridge_outputs() {
		let server = TestServer::spawn(TestMidnightStateService {
			block_by_hash: Ok(BlockByHashResponse {
				block_number: 77,
				tx_count: 0,
				block_timestamp_unix: 0,
				epoch_number: 0,
				slot_number: 0,
			}),
			bridge_utxos: Ok(BridgeUtxosResponse {
				utxos: vec![
					BridgeUtxo {
						tx_hash: vec![0x31; 32],
						output_index: 0,
						block_number: 75,
						block_hash: vec![0x41; 32],
						tx_index: 1,
						block_timestamp_unix: 1_700_000_100,
						tokens_out: 5,
						tokens_in: 5,
						datum: Some(reserve_transfer_datum_bytes()),
					},
					BridgeUtxo {
						tx_hash: vec![0x32; 32],
						output_index: 1,
						block_number: 76,
						block_hash: vec![0x42; 32],
						tx_index: 2,
						block_timestamp_unix: 1_700_000_101,
						tokens_out: 10,
						tokens_in: 4,
						datum: None,
					},
					BridgeUtxo {
						tx_hash: vec![0x33; 32],
						output_index: 2,
						block_number: 77,
						block_hash: vec![0x43; 32],
						tx_index: 3,
						block_timestamp_unix: 1_700_000_102,
						tokens_out: 11,
						tokens_in: 3,
						datum: Some(vec![0x01, 0x02]),
					},
				],
				next_checkpoint: Some(BridgeCheckpoint {
					kind: Some(bridge_checkpoint::Kind::BlockNumber(77)),
				}),
			}),
			last_request: Arc::new(Mutex::new(None)),
		})
		.await;

		let data_source = TokenBridgeDataSourceGrpcImpl::connect(&server.endpoint).await.unwrap();
		let (transfers, checkpoint): (Vec<BridgeTransferV1<[u8; 32]>>, BridgeDataCheckpoint) =
			data_source
				.get_transfers(
					MainChainScripts::default(),
					BridgeDataCheckpoint::Utxo(DomainUtxoId::new([0x55; 32], 7)),
					5,
					McBlockHash([0x77; 32]),
				)
				.await
				.unwrap();

		assert_eq!(
			transfers,
			vec![
				BridgeTransferV1::InvalidTransfer {
					token_amount: 6,
					utxo_id: DomainUtxoId::new([0x32; 32], 1),
				},
				BridgeTransferV1::InvalidTransfer {
					token_amount: 8,
					utxo_id: DomainUtxoId::new([0x33; 32], 2),
				},
			]
		);
		assert_eq!(checkpoint, BridgeDataCheckpoint::Block(sidechain_domain::McBlockNumber(77)));
	}

	fn user_transfer_datum_bytes(recipient: [u8; 32]) -> Vec<u8> {
		let datum: PlutusData =
			TokenTransferDatumV1::UserTransfer { receiver: ByteString(recipient.to_vec()) }.into();
		datum.to_bytes()
	}

	fn reserve_transfer_datum_bytes() -> Vec<u8> {
		let datum: PlutusData = TokenTransferDatumV1::ReserveTransfer.into();
		datum.to_bytes()
	}
}
