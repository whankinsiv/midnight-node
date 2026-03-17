#client
# Fix Acropolis authority-selection gRPC decoding and epoch alignment

Align the Acropolis-backed authority-selection datasource with the db-sync behavior used by the network.

This fixes Ariadne datum decoding for permissioned and registered candidates by decoding full CBOR `PlutusData`, and applies the Cardano data-epoch offset to the actual Acropolis permissioned-candidate, registered-candidate, and epoch-nonce queries instead of only using it for freshness gating.

JIRA: N/A
