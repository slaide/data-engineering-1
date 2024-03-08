sudo apt-get update
sudo apt-get upgrade -fy
sudo apt-get install -y curl tree micro unzip

# install aws cli tools
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# make sure installation worked
aws --version

# set mock backend
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set default.region us-east-1
aws configure set default.output_format json
