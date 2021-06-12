readonly NODE_ID=sn-$1
readonly RABBITMQ_HOST=10.0.0.4
readonly STORAGE_PATH_DOCKER="/app/tmp"
readonly STORAGE_PATH_HOST=/home/nacho/Documents/test/storage/"$NODE_ID"

docker run \
  --name "$NODE_ID" \
  -d  \
  --network=mynet \
  -e RABBITMQ_HOST="$RABBITMQ_HOST" \
  -e NODE_ID="$NODE_ID" \
  -e STORAGE_PATH="$STORAGE_PATH_DOCKER" \
  -l storage \
  -v  "$STORAGE_PATH_HOST":"$STORAGE_PATH_DOCKER" \
  nachocode/storage-node