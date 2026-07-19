# Referencing key pair as defined in variables.tf
data "aws_key_pair" "existing" {
  key_name = var.key_pair_name
}

resource "aws_security_group" "ansible_control_sg" {
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
    Project = "fix-project"
  }
}

resource "aws_iam_role" "ansible_control_role" {
  name = "ansible-control-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ansible_control_policy" {
  name   = "terraform-and-ec2-describe"
  role   = aws_iam_role.ansible_control_role.id
  policy = file("${path.module}/../../../bootstrap/iam/ec2-instance-policy.json")
}

resource "aws_iam_instance_profile" "ansible_control_profile" {
  name = "ansible-control-profile"
  role = aws_iam_role.ansible_control_role.name
}

# Security group: SSH only, locked to your IP
resource "aws_security_group" "ws_client_sg" {
  name        = "ws-client-sg"
  description = "Allow SSH from my IP only"

  ingress {
    description     = "SSH from Ansible control node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_control_sg.id]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ws-client-sg"
  }
}

resource "aws_instance" "ws_client" {
  count                  = var.instance_count
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.ws_client_sg.id]

  tags = {
    Name = "ws-client-${count.index}"
  }
}