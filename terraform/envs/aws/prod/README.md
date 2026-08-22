# Terraform

Terraform state is stored in a dynamo db table within an s3 object, as specified in the backend.tf file.

# Initial terraform commands to provision servers through terraform:

1. terraform init

2. terraform fmt

3. terraform validate

4. terraform plan -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32" -var "aws_profile=binance-fix-project"

5. terraform apply -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32" -var "aws_profile=binance-fix-project"

# Terraform command to destroy (servers, security groups, roles, etc.) through terraform:

6. terraform destroy -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32" -var "aws_profile=binance-fix-project"