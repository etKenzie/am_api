#!/bin/bash

# AWS ECS Deployment Script
# Make sure you have AWS CLI configured and Docker installed

set -e

# Configuration
ECR_REPOSITORY_NAME="akumaju-api"
AWS_REGION="us-east-1"  # Change to your preferred region
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting deployment to AWS ECS...${NC}"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS CLI is not configured. Please run 'aws configure' first.${NC}"
    exit 1
fi

# Create ECR repository if it doesn't exist
echo -e "${YELLOW}Creating ECR repository if it doesn't exist...${NC}"
aws ecr create-repository --repository-name $ECR_REPOSITORY_NAME --region $AWS_REGION 2>/dev/null || echo "Repository already exists"

# Get ECR login token
echo -e "${YELLOW}Logging in to ECR...${NC}"
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build Docker image
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t $ECR_REPOSITORY_NAME .

# Tag image for ECR
ECR_IMAGE_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:latest"
docker tag $ECR_REPOSITORY_NAME:latest $ECR_IMAGE_URI

# Push image to ECR
echo -e "${YELLOW}Pushing image to ECR...${NC}"
docker push $ECR_IMAGE_URI

echo -e "${GREEN}Deployment completed successfully!${NC}"
echo -e "${GREEN}Image URI: $ECR_IMAGE_URI${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo -e "1. Create an ECS cluster"
echo -e "2. Create an ECS task definition using the image URI above"
echo -e "3. Create an ECS service"
echo -e "4. Configure load balancer and target group"
echo -e ""
echo -e "For EC2 deployment:"
echo -e "1. Launch an EC2 instance with Docker installed"
echo -e "2. Pull the image: docker pull $ECR_IMAGE_URI"
echo -e "3. Run the container: docker run -d -p 8000:8000 $ECR_IMAGE_URI" 