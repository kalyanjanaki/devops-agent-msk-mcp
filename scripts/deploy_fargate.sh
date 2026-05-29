#!/usr/bin/env bash
# Manual ECS Fargate deploy for the POC.
# IaC (CDK / CloudFormation) comes after the walking skeleton is validated.
#
# Required env vars:
#   AWS_REGION                     e.g. us-east-1
#   AWS_ACCOUNT_ID                 e.g. 123456789012
#   ECR_REPO                       ECR repo name (must already exist)
#   ECS_CLUSTER                    ECS cluster name (must already exist)
#   ECS_SERVICE                    ECS service name (must already exist)
#   TASK_FAMILY                    ECS task definition family
#   TASK_EXECUTION_ROLE_ARN        Pulls the image, ships logs to CloudWatch
#   TASK_ROLE_ARN                  In-task role with kafka-cluster:* read perms
#   CLUSTERS_YAML_S3_URI           Optional: where to fetch clusters.yaml from at task start
#                                  (a small entrypoint wrapper would download it; for POC, bake it in)
set -euo pipefail

: "${AWS_REGION:?}"
: "${AWS_ACCOUNT_ID:?}"
: "${ECR_REPO:?}"
: "${ECS_CLUSTER:?}"
: "${ECS_SERVICE:?}"
: "${TASK_FAMILY:?}"
: "${TASK_EXECUTION_ROLE_ARN:?}"
: "${TASK_ROLE_ARN:?}"

cd "$(dirname "$0")/.."

IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:$(git rev-parse --short HEAD 2>/dev/null || date +%s)"

echo "1/4 Logging in to ECR"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "2/4 Building image"
docker build -t "$IMAGE_URI" .

echo "3/4 Pushing image"
docker push "$IMAGE_URI"

echo "4/4 Registering new task definition + updating service"
TASK_DEF_JSON=$(cat <<JSON
{
  "family": "$TASK_FAMILY",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "$TASK_EXECUTION_ROLE_ARN",
  "taskRoleArn": "$TASK_ROLE_ARN",
  "containerDefinitions": [
    {
      "name": "msk-mcp",
      "image": "$IMAGE_URI",
      "essential": true,
      "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/msk-mcp",
          "awslogs-region": "$AWS_REGION",
          "awslogs-stream-prefix": "msk-mcp",
          "awslogs-create-group": "true"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "wget -q -O - http://localhost:8080/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 30
      }
    }
  ]
}
JSON
)

NEW_REVISION=$(aws ecs register-task-definition \
  --region "$AWS_REGION" \
  --cli-input-json "$TASK_DEF_JSON" \
  --query 'taskDefinition.taskDefinitionArn' --output text)

echo "Registered $NEW_REVISION"

aws ecs update-service \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --service "$ECS_SERVICE" \
  --task-definition "$NEW_REVISION" \
  --force-new-deployment >/dev/null

echo "Service update kicked off. Tail with:"
echo "  aws ecs describe-services --cluster $ECS_CLUSTER --services $ECS_SERVICE --region $AWS_REGION"
