import { CompiledContract, ContractExecutable, type Contract } from '@midnight-ntwrk/compact-js/effect';
import { Contract as WelcomeContract_ } from './out/contract/index.js';

// The deployer is the organizer; its key is stored as hex (Uint8Array does not round-trip
// through the JSON private-state file) and converted to bytes in the witness.
type WelcomePrivateState = {
  readonly organizerSecretKey: string | null;
  readonly participantId: string | null;
};

type WelcomeContract = WelcomeContract_<WelcomePrivateState>;
const WelcomeContract = WelcomeContract_;

const witnesses: Contract.Contract.Witnesses<WelcomeContract> = {
  // Returns the caller's secret key when they are an organizer, otherwise `none`.
  local_sk: ({ privateState }) => [
    privateState,
    privateState.organizerSecretKey
      ? { is_some: true, value: new Uint8Array(Buffer.from(privateState.organizerSecretKey, 'hex')) }
      : { is_some: false, value: new Uint8Array(32) },
  ],
  // Records the identity used to check in.
  set_local_id: ({ privateState }, participantId) => [{ ...privateState, participantId }, []],
};

const createInitialPrivateState: () => WelcomePrivateState = () => ({
  organizerSecretKey: '{{ORGANIZER_SK}}',
  participantId: null,
});

export default {
  contractExecutable: CompiledContract.make<WelcomeContract>(
    'WelcomeContract',
    WelcomeContract,
  ).pipe(
    CompiledContract.withWitnesses(witnesses),
    CompiledContract.withCompiledFileAssets('./out'),
    ContractExecutable.make,
  ),
  createInitialPrivateState,
  config: {
    keys: {
      coinPublic: '{{COIN_PUBLIC}}',
    },
    network: '{{NETWORK}}',
  },
};
