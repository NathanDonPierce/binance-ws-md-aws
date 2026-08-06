variable "listener_count" {
  description = "Desired number of k3s agent nodes (listeners)"
  type        = number
  default     = 5
}

variable "key_pair_name" {
  description = "Name of existing EC2 key pair used for SSH access"
  type        = string
}

variable "my_ip_cidr" {
  description = "IP in CIDR notation, for SSH access to the Ansible control node"
  type        = string
}

variable "project_name" {
  description = "Project tag applied to all resources"
  type        = string
  default     = "ws-project"
}
