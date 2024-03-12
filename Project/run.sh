bash install-docker.sh
bash install-aws.sh

sudo docker-compose -f localstack-s3-docker-compose.yaml up -d

bash test-aws.sh
