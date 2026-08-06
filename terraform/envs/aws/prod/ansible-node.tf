# Ansible Control Node
resource "aws_security_group" "ansible_node_sg" {
  name        = "ansible-node-sg"
  description = "Security group for the Ansible node"
  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name    = "ansible-node-sg"
    Project = var.project_name
  }
}

resource "aws_iam_role" "ansible_node_role" {
  name = "ansible-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy" "ansible_node_policy" {
  name   = "terraform-and-ec2-manage"
  role   = aws_iam_role.ansible_node_role.id
  policy = file("${path.module}/../../../../iam/ec2-instance-policy.json")
}

resource "aws_iam_instance_profile" "ansible_node_profile" {
  name = "ansible-node-profile"
  role = aws_iam_role.ansible_node_role.name
}

resource "aws_instance" "ansible_node" {
  ami                    = var.ami_id
  instance_type          = var.ansible_instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.ansible_node_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ansible_node_profile.name
  subnet_id              = data.aws_subnet.default.id
  tags = {
    Name    = "ansible-node"
    Project = var.project_name
    Role    = "ansible-node"
  }
}
