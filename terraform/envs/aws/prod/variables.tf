variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-northeast-1"
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.micro"
}

variable "key_pair_name" {
  description = "Name of existing ssh key"
  type        = string
}

variable "ssh_user" {
  description = "Default SSH user"
  type        = string
  default     = "ec2-user"
}

variable "ami_id" {
  description = "AMI ID for RHEL 10 in aws-apnortheast1"
  type        = string
  default     = "ami-08d0fa6d084fda9db"
}

variable "my_ip_cidr" {
  description = "Your IP in CIDR notation, for SSH access (e.g. 203.0.113.5/32)"
  type        = string
}

variable "aws_profile" {
  description = "aws_profile added to terraform.tfvars"
  type        = string
}

variable "instance_count" {
  description = "Number of ws-client EC2 instances to launch"
  type        = number
  default     = 3
}