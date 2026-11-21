#!/usr/bin/env bash
# Create the S3 bucket + least-privilege IAM user for the lakehouse.
# Requires the AWS CLI, logged in as an admin:  aws configure
set -euo pipefail
BUCKET="${1:?usage: aws/setup_s3.sh <globally-unique-bucket-name> [region]}"
REGION="${2:-us-east-1}"
USER_NAME="wiki-lakehouse"

if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration file://"$(dirname "$0")/lifecycle.json"

POLICY=$(sed "s/YOUR-BUCKET-NAME/$BUCKET/g" "$(dirname "$0")/iam-policy.json")
aws iam create-user --user-name "$USER_NAME" >/dev/null
aws iam put-user-policy --user-name "$USER_NAME" --policy-name wiki-lakehouse-s3 --policy-document "$POLICY"
echo "Access key for .env (shown once):"
aws iam create-access-key --user-name "$USER_NAME" \
  --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
echo "Set WAREHOUSE=s3://$BUCKET/warehouse and S3_ENDPOINT=https://s3.$REGION.amazonaws.com in .env"
