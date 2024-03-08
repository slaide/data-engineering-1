bash install_docker.sh
bash install_aws.sh

docker-compose -d up localstack-s3-docker-compose.yaml

bash test-aws.sh
