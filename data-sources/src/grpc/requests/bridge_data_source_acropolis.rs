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

use sidechain_domain::McBlockHash;
use sp_partner_chains_bridge::{BridgeDataCheckpoint, BridgeTransferV1};
use tonic::{Status, transport::Channel};

use crate::grpc::{
	conversions::{
		bridge_checkpoint_from_proto, bridge_checkpoint_to_proto, bridge_transfer_from_proto,
	},
	midnight_state::{BridgeTransfersRequest, midnight_state_client::MidnightStateClient},
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
	let response = client
		.get_bridge_transfers(BridgeTransfersRequest {
			checkpoint: Some(bridge_checkpoint_to_proto(data_checkpoint)?),
			current_block_hash: current_mc_block_hash.0.to_vec(),
			transfer_capacity: max_transfers,
		})
		.await?
		.into_inner();

	let transfers = response
		.transfers
		.into_iter()
		.map(bridge_transfer_from_proto)
		.collect::<Result<Vec<_>, _>>()?
		.into_iter()
		.flatten()
		.collect();

	let next_checkpoint = bridge_checkpoint_from_proto(
		response
			.next_checkpoint
			.ok_or_else(|| Status::internal("BridgeTransfersResponse missing next_checkpoint"))?,
	)?;

	Ok((transfers, next_checkpoint))
}
