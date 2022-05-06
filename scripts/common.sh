#!/bin/bash

# fail if anything fails
set -ex

SPORKS_JSON="https://raw.githubusercontent.com/onflow/flow/master/sporks.json"

function bootstrap() {
  NETWORK_TYPE=$1
  NETWORK_NUMBER=$2
  DIR=$3

  BOOTSTRAP_DIR="$DIR/bootstrap"
  mkdir -p "$BOOTSTRAP_DIR"

  PUBLIC_ROOT_INFO_DIR="$BOOTSTRAP_DIR/public-root-information"
  mkdir -p "$PUBLIC_ROOT_INFO_DIR"

  SPORK_DATA=$(curl -s $SPORKS_JSON | jq .networks."$NETWORK_TYPE"."$NETWORK_TYPE""$NETWORK_NUMBER")

  ROOT_CHECKPOINT_URL=$(jq -r .stateArtefacts.gcp.rootCheckpointFile <<< "$SPORK_DATA")
  ROOT_PROTOCOL_STATE_SNAPSHOT_URL=$(jq -r .stateArtefacts.gcp.rootProtocolStateSnapshot <<< "$SPORK_DATA")

  ROOT_CHECKPOINT_FILE="$BOOTSTRAP_DIR/root.checkpoint"
  ROOT_PROTOCOL_STATE_SNAPSHOT_FILE="$PUBLIC_ROOT_INFO_DIR/root-protocol-state-snapshot.json"

  if [ ! -f "$ROOT_CHECKPOINT_FILE" ]; then
    curl -o "$ROOT_CHECKPOINT_FILE" $ROOT_CHECKPOINT_URL
  fi

  if [ ! -f "$ROOT_PROTOCOL_STATE_SNAPSHOT_FILE" ]; then
    curl -o "$ROOT_PROTOCOL_STATE_SNAPSHOT_FILE" "$ROOT_PROTOCOL_STATE_SNAPSHOT_URL"
  fi

  GCP_BUCKET=$(jq -r .stateArtefacts.gcp.executionStateBucket <<< "$SPORK_DATA")

  # this selects random node from the list
  SEED_NODES_COUNT=$(jq -r .seedNodes\|length <<< "$SPORK_DATA")
  # shellcheck disable=SC2004
  SELECTED_NODE=$(($RANDOM%SEED_NODES_COUNT))

  SEED_ADDRESS=$(jq -r .seedNodes[$SELECTED_NODE].address <<< "$SPORK_DATA")
  SEED_KEY=$(jq -r .seedNodes[$SELECTED_NODE].key <<< "$SPORK_DATA")
}