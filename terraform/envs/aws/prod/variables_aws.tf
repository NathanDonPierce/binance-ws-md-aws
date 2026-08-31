variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-northeast-1"
}


variable "server_instance_type" {
  description = "EC2 instance type for the orchestrator and kafka node"
  type        = string
  default     = "c7i-flex.large"
}

variable "agent_instance_type" {
  description = "EC2 instance type for listener nodes"
  type        = string
  default     = "t3.small"
}

variable "ansible_instance_type" {
  description = "EC2 instance type for ansible control node"
  type        = string
  default     = "t3.micro"
}

variable "ssh_user" {
  description = "Default SSH user for the chosen AMI"
  type        = string
  default     = "ec2-user"
}

variable "ami_id" {
  description = "AMI ID for RHEL in the target region"
  type        = string
}

variable "listener_ami_id" {
  description = "AMI ID for Amazon Linux OS in the target region - needed for PTP"
  type        = string
}

variable "aws_profile" {
  description = "AWS CLI profile to use."
  type        = string
  default     = ""
}