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

use sidechain_domain::{McBlockHash, McBlockNumber};
use sp_partner_chains_bridge::{BridgeDataCheckpoint, BridgeTransferV1};
use tonic::{Status, transport::Channel};

use crate::grpc::{
	conversions::{
		bridge_checkpoint_from_proto, bridge_checkpoint_to_proto, bridge_utxo_to_transfer,
	},
	midnight_state::{
		BlockByHashRequest, BridgeUtxosRequest, midnight_state_client::MidnightStateClient,
	},
};

pub(crate) async fn get_bridge_transfers<RecipientAddress>(
	client: &mut MidnightStateClient<Channel>,
	data_checkpoint: BridgeDataCheckpoint,
	max_transfers: u32,
	current_mc_block_hash: McBlockHash,
) -> Result<(Vec<BridgeTransferV1<RecipientAddress>>, BridgeDataCheckpoint), Status>
where
	RecipientAddress: for<'a> TryFrom<&'a [u8]>,
{
	let to_block = get_block_number_by_hash(client, current_mc_block_hash).await?;

	let response = client
		.get_bridge_utxos(BridgeUtxosRequest {
			checkpoint: Some(bridge_checkpoint_to_proto(data_checkpoint)?),
			to_block: u64::from(to_block.0),
			utxo_capacity: max_transfers,
		})
		.await?
		.into_inner();

	let transfers = response
		.utxos
		.into_iter()
		.map(bridge_utxo_to_transfer)
		.collect::<Result<Vec<_>, _>>()?
		.into_iter()
		.flatten()
		.collect();

	let next_checkpoint = bridge_checkpoint_from_proto(
		response
			.next_checkpoint
			.ok_or_else(|| Status::internal("BridgeUtxosResponse missing next_checkpoint"))?,
	)?;

	Ok((transfers, next_checkpoint))
}

async fn get_block_number_by_hash(
	client: &mut MidnightStateClient<Channel>,
	block_hash: McBlockHash,
) -> Result<McBlockNumber, Status> {
	let response = client
		.get_block_by_hash(BlockByHashRequest { block_hash: block_hash.0.to_vec() })
		.await?
		.into_inner();

	Ok(McBlockNumber(response.block_number))
}
