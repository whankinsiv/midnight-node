use pallet_sidechain_rpc::SidechainRpcDataSource;
use sidechain_domain::MainchainBlock;
use tonic::transport::{Channel, Endpoint};

use crate::grpc::{midnight_state::midnight_state_client::MidnightStateClient, requests::sidechain_rpc_data_source_acropolis::get_latest_block};

pub struct SidechainRpcDataSourceGrpcImpl {
	pub client: MidnightStateClient<Channel>,
}

impl SidechainRpcDataSourceGrpcImpl {
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
impl SidechainRpcDataSource for SidechainRpcDataSourceGrpcImpl {
	async fn get_latest_block_info(
		&self,
	) -> Result<MainchainBlock, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		get_latest_block(&mut client)
			.await
			.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
	}
}
