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
  default     = "AnsibleHost"
}

variable "ssh_user" {
  description = "Default SSH user"
  type        = string
  default     = "ec2-user"
}