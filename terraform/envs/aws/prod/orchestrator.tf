# Orchestrator: security group, IAM, instance

resource "aws_security_group" "orchestrator" {
  name        = "orchestrator"
  description = "orchestrator: SSH from control node, API from agents and control node, VXLAN from agents"

  ingress {
    description     = "SSH from Ansible node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_node_sg.id]
  }

  ingress {
    description     = "k3s API from Ansible node"
    from_port       = 6443
    to_port         = 6443
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_node_sg.id]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "orchestrator-sg"
    Project = var.project_name
  }
}

# Server IAM Role
resource "aws_iam_role" "orchestrator_role" {
  name = "orchestrator-role"

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

resource "aws_iam_role_policy" "orchestrator_ssm_policy" {
  name = "ssm-join-token-write"
  role = aws_iam_role.orchestrator_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:PutParameter", "ssm:GetParameter"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/binance-ws/*"
    }]
  })
}

resource "aws_iam_instance_profile" "orchestrator_profile" {
  name = "orchestrator-profile"
  role = aws_iam_role.orchestrator_role.name
}

resource "aws_instance" "orchestrator" {
  ami                    = var.ami_id
  instance_type          = var.server_instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.orchestrator.id]
  iam_instance_profile   = aws_iam_instance_profile.orchestrator_profile.name
  subnet_id              = data.aws_subnet.default.id

  tags = {
    Name    = "orchestrator"
    Project = var.project_name
    Role    = "orchestrator"
  }
}

resource "aws_ssm_parameter" "k3s_server_ip" {
  name  = "/binance-ws/k3s-server-ip"
  type  = "String"
  value = aws_instance.orchestrator.private_ip

  tags = {
    Project = var.project_name
  }
}
