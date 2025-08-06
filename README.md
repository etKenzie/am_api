# Aku Maju API

A FastAPI-based resume scoring API service.

## Features

- Health check endpoint
- Resume scoring functionality
- PDF processing capabilities
- JWT authentication support

## Local Development

### Prerequisites

- Python 3.11+
- Docker
- Docker Compose

### Running with Docker Compose

1. Clone the repository
2. Build and run the application:

```bash
docker-compose up --build
```

The API will be available at `http://localhost:8000`

### Running Locally

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
```

## Docker Deployment

### Building the Image

```bash
docker build -t akumaju-api .
```

### Running the Container

```bash
docker run -d -p 8000:8000 akumaju-api
```

## AWS Deployment

### Option 1: AWS ECS (Recommended)

1. **Prerequisites:**
   - AWS CLI configured
   - Docker installed
   - Appropriate AWS permissions

2. **Deploy to ECR:**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

3. **Create ECS Resources:**
   - Create an ECS cluster
   - Create a task definition using the ECR image URI
   - Create an ECS service
   - Configure Application Load Balancer

### Option 2: AWS EC2

1. **Launch EC2 Instance:**
   - Use Amazon Linux 2 or Ubuntu
   - Install Docker: `sudo yum install -y docker` (Amazon Linux) or `sudo apt-get install docker.io` (Ubuntu)
   - Start Docker: `sudo systemctl start docker`

2. **Deploy Application:**
   ```bash
   # Pull the image from ECR
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
   docker pull <account-id>.dkr.ecr.us-east-1.amazonaws.com/akumaju-api:latest
   
   # Run the container
   docker run -d -p 80:8000 <account-id>.dkr.ecr.us-east-1.amazonaws.com/akumaju-api:latest
   ```

3. **Security Group Configuration:**
   - Allow inbound traffic on port 80 (HTTP)
   - Allow inbound traffic on port 443 (HTTPS) if using SSL

### Option 3: AWS ECS with Fargate

1. **Create ECS Cluster:**
   ```bash
   aws ecs create-cluster --cluster-name akumaju-cluster
   ```

2. **Create Task Definition:**
   ```json
   {
     "family": "akumaju-task",
     "networkMode": "awsvpc",
     "requiresCompatibilities": ["FARGATE"],
     "cpu": "256",
     "memory": "512",
     "executionRoleArn": "arn:aws:iam::<account-id>:role/ecsTaskExecutionRole",
     "containerDefinitions": [
       {
         "name": "akumaju-api",
         "image": "<account-id>.dkr.ecr.us-east-1.amazonaws.com/akumaju-api:latest",
         "portMappings": [
           {
             "containerPort": 8000,
             "protocol": "tcp"
           }
         ],
         "healthCheck": {
           "command": ["CMD-SHELL", "curl -f http://localhost:8000/health || exit 1"],
           "interval": 30,
           "timeout": 5,
           "retries": 3,
           "startPeriod": 60
         }
       }
     ]
   }
   ```

3. **Create ECS Service:**
   - Use the task definition above
   - Configure target group for load balancer
   - Set desired count and auto-scaling policies

## Environment Variables

The following environment variables can be configured:

- `PYTHONPATH`: Python path (default: `/app`)
- `PORT`: Application port (default: 8000)

## Health Check

The application includes a health check endpoint:

```
GET /health
```

Returns:
```json
{
  "status": "healthy",
  "service": "resume-scoring-api"
}
```

## API Endpoints

- `GET /` - Root endpoint with API information
- `GET /health` - Health check endpoint

## Monitoring and Logging

- Health checks are configured in the Dockerfile
- Application logs are available via Docker logs
- Consider using CloudWatch for centralized logging in AWS

## Security Considerations

- The Docker container runs as a non-root user
- Health checks are implemented
- Consider using AWS Secrets Manager for sensitive configuration
- Implement proper IAM roles and security groups

## Troubleshooting

### Common Issues

1. **Port already in use:**
   ```bash
   docker ps
   docker stop <container-id>
   ```

2. **ECR login issues:**
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
   ```

3. **Permission denied:**
   - Ensure your AWS credentials have appropriate permissions
   - Check IAM roles and policies

### Logs

View application logs:
```bash
docker logs <container-id>
```

For ECS:
```bash
aws logs describe-log-groups
aws logs tail <log-group-name> --follow
``` 