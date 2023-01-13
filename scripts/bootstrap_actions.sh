sudo yum install -y jq
aws configure set region \
  "$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)"

sudo python3 -m pip install boto3 ec2-metadata