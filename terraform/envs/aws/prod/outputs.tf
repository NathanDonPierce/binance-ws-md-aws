output "ansible_node_public_ip" {
  description = "Public IP of the Ansible node"
  value       = aws_instance.ansible_node.public_ip
}

output "ansible_node_ssh_command" {
  description = "SSH command to reach the ansible node from your laptop"
  value       = "ssh -i ~/.ssh/${var.key_pair_name}.pem ${var.ssh_user}@${aws_instance.ansible_node.public_ip}"
}

output "orchestrator_private_ip" {
  description = "Private IP of the orchestrator node (k3s API, SSH via Ansible jump host)"
  value       = aws_instance.orchestrator.private_ip
}

output "orchestrator_id" {
  description = "Instance ID of the orchestrator node"
  value       = aws_instance.orchestrator.id
}

output "listener_node_asg_names" {
  description = "Name of the listener nodes Auto Scaling Group"
  value       = { for k, v in aws_autoscaling_group.listener_nodes : k => v.name }
}
