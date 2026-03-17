#client
# Read bridge UTXOs from Acropolis gRPC

Switch the node's Acropolis mainchain follower bridge datasource from the mock implementation to the real `GetBridgeUtxos` gRPC flow, vendor the Acropolis bridge protobuf messages needed for that client, and align the Acropolis-backed Ariadne/cNIGHT inherent paths with db-sync behavior.

This also fixes authority-selection epoch offset and datum decoding for Acropolis gRPC, and improves cNIGHT inherent mismatch diagnostics so verifier failures identify the first differing UTxO or position.

JIRA: N/A
