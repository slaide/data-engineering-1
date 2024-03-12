bash install-docker.sh
bash install-aws.sh

docker-compose -d up localstack-s3-docker-compose.yaml

bash test-aws.sh
