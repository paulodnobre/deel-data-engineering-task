#!/bin/sh

CONNECT_URL="http://kafka-connect:8083"
CONNECTORS_DIR="/connectors"

echo "Waiting for Kafka Connect to be ready..."
until curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors" | grep -q "200"; do
  echo "Kafka Connect not ready yet, retrying in 5s..."
  sleep 5
done

echo "Kafka Connect is ready. Registering connectors..."

for CONNECTOR_FILE in "${CONNECTORS_DIR}"/*.json; do
  CONNECTOR_NAME=$(basename "${CONNECTOR_FILE}" .json)
  echo "Creating connector from ${CONNECTOR_NAME}..."

  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @"${CONNECTOR_FILE}")

  HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)
  BODY=$(echo "${RESPONSE}" | sed '$d')

  if [ "${HTTP_CODE}" = "201" ]; then
    echo "Connector ${CONNECTOR_NAME} created successfully."
  elif [ "${HTTP_CODE}" = "409" ]; then
    echo "Connector ${CONNECTOR_NAME} already exists, skipping."
  else
    echo "Failed to create connector ${CONNECTOR_NAME} (HTTP ${HTTP_CODE}): ${BODY}"
  fi
done

echo ""
echo "All connectors processed. Current status:"
curl -s "${CONNECT_URL}/connectors?expand=status"
