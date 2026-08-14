# Docker Setup & Deployment Guide

## Local Development with Docker Compose

### Prerequisites
- Docker Desktop installed
- Docker Compose installed (included with Docker Desktop)
- Clone the repository

### Running Locally

#### 1. Start PostgreSQL only (for development)
```bash
docker-compose up postgres -d
```

#### 2. Start full stack (PostgreSQL + Pipeline + pgAdmin)
```bash
# Copy environment template
cp .env.example .env

# Start services
docker-compose --profile with-pipeline --profile with-pgadmin up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

#### 3. Access Services
- **PostgreSQL**: `localhost:5432` (use credentials from `.env`)
- **pgAdmin**: `http://localhost:5050` (admin@example.com / admin)

### Building the Docker Image Locally

```bash
# Build image
docker build -t ecommerce-pipeline:latest .

# Run container with volume mounts
docker run -it \
  -e PG_HOST=host.docker.internal \
  -e PG_USER=postgres \
  -e PG_PASSWORD=postgres \
  -v C:/ecommerce_delta_lake:/data/delta_lake \
  -v C:/path/to/data:/app/data:ro \
  ecommerce-pipeline:latest
```

## CI/CD Pipelines

### GitHub Actions Workflows

#### CI Workflow (`.github/workflows/ci.yml`)
Triggered on:
- Every push to `main` or `develop`
- Every pull request

What it does:
1. **Lint**: Code formatting checks with Ruff
2. **Unit Tests**: Run all unit tests (~/40 tests)
3. **Integration Tests**: Run E2E tests with PostgreSQL service
4. **Build**: Build Docker image (don't push to registry)

#### CD Workflow (`.github/workflows/cd.yml`)
Triggered on:
- Push to `main` branch
- Tag pushes (`v*`)

What it does:
1. Build Docker image with optimized caching
2. Push to Azure Container Registry
3. Run smoke tests on deployed image
4. Send Slack notification (if webhook configured)

### Required GitHub Secrets

Set these in repository Settings → Secrets:

```
AZURE_CREDENTIALS          # Service principal credentials (JSON)
ACR_REGISTRY_NAME          # Azure Container Registry name (e.g., myregistry)
ACR_REGISTRY_URL           # Full ACR URL (e.g., myregistry.azurecr.io)
SLACK_WEBHOOK_URL          # (Optional) Slack webhook for notifications
```

### Azure Setup

1. Create Azure Container Registry:
```bash
az acr create --resource-group <rg> --name <name> --sku Basic
az acr login --name <name>
```

2. Get ACR URL:
```bash
az acr show --name <name> --query loginServer --output tsv
```

3. Create service principal for CI/CD:
```bash
az ad sp create-for-rbac \
  --name "github-pipeline-sp" \
  --role AcrPush \
  --scopes /subscriptions/<sub-id>/resourceGroups/<rg>
```

4. Add the returned credentials as `AZURE_CREDENTIALS` secret in GitHub

## Production Deployment

### Deploy with Azure Container Registry

```bash
# Pull latest image
az acr login --name <your-acr>
docker pull <your-acr>.azurecr.io/ecommerce-pipeline:latest

# Run with environment configuration
docker run -d \
  --name ecommerce-pipeline \
  -e PG_HOST=<prod-postgres-host> \
  -e PG_DATABASE=ecommerce_db \
  -e PG_USER=<username> \
  -e PG_PASSWORD=<password> \
  -v /mnt/data:/data/delta_lake \
  <your-acr>.azurecr.io/ecommerce-pipeline:latest
```

### Docker Image Details

**Image Size**: ~500MB (slim base + dependencies)

**Layers**:
- Stage 1: Builder (compiles dependencies)
- Stage 2: Runtime (minimal image with only runtime dependencies)

**Entrypoint**: Runs Prefect flow by default
```bash
python -m src.flows.prefect_flow
```

**Health Check**: Validates Python imports every 60 seconds

## Troubleshooting

### Container won't connect to PostgreSQL
- Ensure `postgres` service is healthy: `docker-compose ps`
- Check network: `docker network ls` and `docker network inspect ecommerce-network`
- Verify credentials in `.env` match container env vars

### Build fails with "uv: command not found"
- Rebuild without cache: `docker build --no-cache -t ecommerce-pipeline .`

### CI/CD Pipeline stuck
- Check GitHub Actions logs in repository
- Verify secrets are set correctly (especially Azure credentials)
- Ensure ACR exists and is accessible

## Best Practices

✅ **DO**:
- Use multi-stage builds (reduces image size)
- Tag images with commit SHA for traceability
- Use docker-compose profiles for optional services
- Run containers as non-root user
- Include health checks

❌ **DON'T**:
- Include credentials in Dockerfile or image
- Use latest tags in production
- Skip testing before deploying
- Commit `.env` files
