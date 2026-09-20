#!/usr/bin/env bash
set -euo pipefail

# Tears down what provision_aws.sh created. Run this whenever you're
# done for a while — RDS bills hourly even idle, free-tier runs out.

REGION="af-south-1"
DB_INSTANCE_ID="retail-lakehouse-db"

read -p "Bucket name to delete (from provision_aws.sh output): " BUCKET_NAME

echo "Deleting RDS instance..."
aws rds delete-db-instance \
  --db-instance-identifier "${DB_INSTANCE_ID}" \
  --skip-final-snapshot \
  --region "${REGION}"

echo "Emptying and deleting S3 bucket..."
aws s3 rb "s3://${BUCKET_NAME}" --force --region "${REGION}"

echo "Done. Double-check RDS + S3 consoles in ${REGION}."