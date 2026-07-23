# Referencing key pair as defined in variables.tf
data "aws_key_pair" "existing" {
  key_name = var.key_pair_name
}

# --- Ansible control node ---
resource "aws_security_group" "ansible_control_node_sg" {
  name        = "ansible-control-sg"
  description = "Security group for the Ansible control node"
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
    Name    = "ansible-control-sg"
    Project = var.project_name
  }
}

resource "aws_iam_role" "ansible_control_node_role" {
  name = "ansible-control-role"

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

resource "aws_iam_role_policy" "ansible_control_node_policy" {
  name   = "terraform-and-ec2-manage"
  role   = aws_iam_role.ansible_control_node_role.id
  policy = file("${path.module}/../../../../iam/ec2-instance-policy.json")
}

resource "aws_iam_instance_profile" "ansible_control_node_profile" {
  name = "ansible-control-profile"
  role = aws_iam_role.ansible_control_node_role.name
}

resource "aws_instance" "ansible_control_node" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.ansible_control_node_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ansible_control_node_profile.name
  tags = {
    Name    = "ansible-control"
    Project = var.project_name
    Role    = "control-node"
  }
}

# --- ws-client instances (control node access only) ---
resource "aws_security_group" "ws_client_sg" {
  name        = "ws-client-sg"
  description = "Allow SSH only from the Ansible control node"
  ingress {
    description     = "SSH from Ansible control node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_control_node_sg.id]
  }

  ingress {
    description     = "k3s API from Ansible control node"
    from_port       = 6443
    to_port         = 6443
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_control_node_sg.id]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name    = "ws-client-sg"
    Project = var.project_name
  }
}

resource "aws_instance" "ws_client" {
  count                  = var.instance_count
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.ws_client_sg.id]
  tags = {
    Name    = "ws-client-${count.index}"
    Project = var.project_name
    Role    = "ws-client"
  }
}