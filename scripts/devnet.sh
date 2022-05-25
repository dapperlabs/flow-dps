source "$(dirname "$0")/common.sh"

DATA_DIR="/var/flow/dps/testnet34"

# bootstrap function should set those variables

GCP_BUCKET=""
SEED_ADDRESS=""
SEED_KEY=""

bootstrap "testnet" 34 "$DATA_DIR"

# make sure you build a docker image first
# dir where data has been downloaded should be mounted
docker run \
  -v $DATA_DIR/:/data/ \
  -p 5005:5005 \
  --name flow-dps \
  gcr.io/flow-container-registry/flow-dps-live:v0.25 \
  -a 0.0.0.0:5005 \
  -i /data/index \
  -b /data/bootstrap \
  -c /data/bootstrap/root.checkpoint \
  -d /data/protocol \
  -l info \
  -u $GCP_BUCKET \
  --seed-address="$SEED_ADDRESS" \
  --seed-key="$SEED_KEY"

