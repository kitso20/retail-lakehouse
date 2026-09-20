#!/usr/bin/env bash
set -euo pipefail

# Provisions real AWS infrastructure for the retail lakehouse:
#   1. S3 bucket (bronze zone) — replaces local MinIO
#   2. A security group allowing Postgres (5432) ONLY from your current IP
#   3. An RDS Postgres instance (free-tier eligible) — replaces local warehouse-db
#
# Run locally with AWS CLI configured (aws configure, region af-south-1).

REGION="af-south-1"
BUCKET_NAME="retail-lakehouse-$(whoami)-$(date +%Y%m%d)"
DB_INSTANCE_ID="retail-lakehouse-db"
DB_NAME="retail"
DB_USERNAME="retail_user"
DB_INSTANCE_CLASS="db.t4g.micro"
DB_STORAGE_GB=20

echo "== Step 1: Checking your current public IP =="
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "Your IP: ${MY_IP}"

echo "== Step 2: Creating S3 bucket =="
aws s3api create-bucket \
  --bucket "${BUCKET_NAME}" \
  --region "${REGION}" \
  --create-bucket-configuration LocationConstraint="${REGION}"
echo "Bucket created: s3://${BUCKET_NAME}"

echo "== Step 3: Security group scoped to YOUR IP only =="
VPC_ID=$(aws ec2 describe-vpcs --region "${REGION}" --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId" --output text)
SG_ID=$(aws ec2 create-security-group \
  --group-name retail-lakehouse-db-sg \
  --description "Postgres access for retail lakehouse - dev only" \
  --vpc-id "${VPC_ID}" \
  --region "${REGION}" \
  --query "GroupId" --output text)

aws ec2 authorize-security-group-ingress \
  --group-id "${SG_ID}" \
  --protocol tcp --port 5432 \
  --cidr "${MY_IP}/32" \
  --region "${REGION}"
echo "Security group created: ${SG_ID} (open to ${MY_IP}/32 only)"

echo "== Step 4: DB password =="
read -s -p "Enter a strong password for the RDS master user: " DB_PASSWORD
echo

echo "== Step 5: Creating RDS Postgres instance (5-10 min) =="
aws rds create-db-instance \
  --db-instance-identifier "${DB_INSTANCE_ID}" \
  --db-name "${DB_NAME}" \
  --db-instance-class "${DB_INSTANCE_CLASS}" \
  --engine postgres \
  --engine-version 16.3 \
  --master-username "${DB_USERNAME}" \
  --master-user-password "${DB_PASSWORD}" \
  --allocated-storage "${DB_STORAGE_GB}" \
  --vpc-security-group-ids "${SG_ID}" \
  --publicly-accessible \
  --region "${REGION}" \
  --backup-retention-period 1 \
  --no-multi-az

echo "Check status:"
echo "  aws rds describe-db-instances --db-instance-identifier ${DB_INSTANCE_ID} --region ${REGION} --query 'DBInstances[0].DBInstanceStatus'"
echo ""
echo "Once 'available', get endpoint:"
echo "  aws rds describe-db-instances --db-instance-identifier ${DB_INSTANCE_ID} --region ${REGION} --query 'DBInstances[0].Endpoint.Address' --output text"
echo ""
echo "Then update .env:"
echo "  WAREHOUSE_DB_HOST=<endpoint above>"
echo "  WAREHOUSE_DB_PORT=5432"
echo "  WAREHOUSE_DB_NAME=${DB_NAME}"
echo "  WAREHOUSE_DB_USER=${DB_USERNAME}"
echo "  WAREHOUSE_DB_PASSWORD=<password you entered>"
echo "  S3_BUCKET=${BUCKET_NAME}"
echo "  S3_ENDPOINT_URL=   <-- LEAVE BLANK for real AWS S3"
echo ""
echo "To tear down later:"
echo "  aws rds delete-db-instance --db-instance-identifier ${DB_INSTANCE_ID} --skip-final-snapshot --region ${REGION}"
echo "  aws s3 rb s3://${BUCKET_NAME} --force --region ${REGION}"