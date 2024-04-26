docker login -u ${DOCKERHUB_USERNAME} -p ${DOCKERHUB_PASSWORD}
sudo mkdir -p /var/lib/app/${BITBUCKET_REPO_SLUG}
sudo chown -R ${DEFAULT_SSH_USER}:${DEFAULT_SSH_USER} /var/lib/app/${BITBUCKET_REPO_SLUG}
mv docker-compose.yaml /var/lib/app/${BITBUCKET_REPO_SLUG}
docker-compose -f /var/lib/app/${BITBUCKET_REPO_SLUG}/docker-compose.yaml down --rmi all
docker-compose -f /var/lib/app/${BITBUCKET_REPO_SLUG}/docker-compose.yaml pull
docker-compose -f /var/lib/app/${BITBUCKET_REPO_SLUG}/docker-compose.yaml up -d