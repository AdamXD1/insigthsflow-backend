export REGION=us-central1

export GOOGLE_CLOUD_PROJECT=insightsflow-backend
image_tag="${REGION}-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/insightsflow-repo/insigthsflow-backend-fastapi:latest"

gcloud auth --project ${GOOGLE_CLOUD_PROJECT} configure-docker "${REGION}-docker.pkg.dev"

docker build -t "${image_tag}"  .
docker push "$image_tag"


gcloud run deploy "insigthsflow-backend-fastapi" \
    --image "${image_tag}" \
    --project "${GOOGLE_CLOUD_PROJECT}" \
    --region "${REGION}" \
    --platform managed \
    --allow-unauthenticated
