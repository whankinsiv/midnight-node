use partner_chains_db_sync_data_sources::{DbSyncBlockDataSourceConfig, McFollowerMetrics};
use sidechain_domain::{MainchainBlock, McBlockHash};
use sidechain_mc_hash::McHashDataSource;
use sp_timestamp::Timestamp;
use tonic::transport::{Channel, Endpoint};

use crate::{
	data_source::candidates_data_source::observed_async_trait,
	grpc::requests::mc_hash_data_source_acropolis::{
		get_block_by_hash, get_latest_stable_block, get_stable_block,
	},
	midnight_state::midnight_state_client::MidnightStateClient,
};

pub struct McHashDataSourceGrpcImpl {
	pub client: MidnightStateClient<Channel>,
	/// Cardano security parameter
	///
	/// This parameter controls how many confirmations (blocks on top) are required by
	/// the Cardano node to consider a block to be stable. This is a network-wide parameter.
	security_parameter: u32,
	/// Additional offset applied when selecting the latest stable Cardano block
	///
	/// This parameter should be 0 by default and should only be increased to 1 in networks
	/// struggling with frequent block rejections due to Db-Sync or Cardano node lag.
	block_stability_margin: u32,

	/// Prometheus metrics client
	metrics_opt: Option<McFollowerMetrics>,
}
impl McHashDataSourceGrpcImpl {
	pub async fn connect(
		endpoint: impl AsRef<str>,
		config: DbSyncBlockDataSourceConfig,
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

		Ok(Self {
			client: MidnightStateClient::new(channel),
			security_parameter: config.cardano_security_parameter,
			block_stability_margin: config.block_stability_margin,
			metrics_opt: None,
		})
	}
}
observed_async_trait!(
impl McHashDataSource for McHashDataSourceGrpcImpl {
	async fn get_latest_stable_block_for(
		&self,
		_reference_timestamp: Timestamp,
	) -> Result<Option<MainchainBlock>, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let offset = self.security_parameter + self.block_stability_margin;
		get_latest_stable_block(&mut client, offset)
			.await
			.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
	}

	async fn get_stable_block_for(
		&self,
		hash: McBlockHash,
		_reference_timestamp: Timestamp,
	) -> Result<Option<MainchainBlock>, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		let offset = self.security_parameter + self.block_stability_margin;
		get_stable_block(&mut client, hash, offset)
			.await
			.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
	}

	async fn get_block_by_hash(
		&self,
		hash: McBlockHash,
	) -> Result<Option<MainchainBlock>, Box<dyn std::error::Error + Send + Sync>> {
		let mut client = self.client.clone();
		get_block_by_hash(&mut client, hash)
			.await
			.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
	}
});
