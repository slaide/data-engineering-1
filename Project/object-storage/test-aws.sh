awslocal(){
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=${DEFAULT_REGION:-$AWS_DEFAULT_REGION} aws --endpoint-url=http://${LOCALSTACK_HOST:-localhost}:4566 "$@"
}

# create an s3 bucket
awslocal s3 mb s3://my-test-bucket
# upload a file
awslocal s3 cp test.txt s3://my-test-bucket/
# download a file
awslocal s3 cp s3://my-test-bucket/test.txt downloaded-test.txt
