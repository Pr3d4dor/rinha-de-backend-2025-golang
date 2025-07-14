docker login
docker buildx build --platform linux/amd64 -t pr3d4dor/rinha-de-backend-2025-golang:latest .
docker push pr3d4dor/rinha-de-backend-2025-golang:latest
