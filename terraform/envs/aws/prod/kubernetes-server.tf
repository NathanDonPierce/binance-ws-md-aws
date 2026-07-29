# Kubernetes server: security group, IAM, instance

resource "aws_security_group" "kubernetes_server" {
  name        = "kubernetes-server"
  description = "kubernetes server: SSH from control node, API from agents and control node, VXLAN from agents"

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
    Name    = "kubernetes-server-sg"
    Project = var.project_name
  }
}

# Server IAM Role
resource "aws_iam_role" "kubernetes_server_role" {
  name = "kubernetes-server-role"

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

resource "aws_iam_role_policy" "kubernetes_server_ssm_policy" {
  name = "ssm-join-token-write"
  role = aws_iam_role.kubernetes_server_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:PutParameter", "ssm:GetParameter"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/binance-ws/*"
    }]
  })
}

resource "aws_iam_instance_profile" "kubernetes_server_profile" {
  name = "kubernetes-server-profile"
  role = aws_iam_role.kubernetes_server_role.name
}

resource "aws_instance" "kubernetes_server" {
  ami                    = var.ami_id
  instance_type          = var.server_instance_type
  key_name               = data.aws_key_pair.existing.key_name
  vpc_security_group_ids = [aws_security_group.kubernetes_server.id]
  iam_instance_profile   = aws_iam_instance_profile.kubernetes_server_profile.name
  subnet_id              = data.aws_subnet.default.id

  tags = {
    Name    = "kubernetes-server"
    Project = var.project_name
    Role    = "server"
  }
}

resource "aws_ssm_parameter" "k3s_server_ip" {
  name  = "/binance-ws/k3s-server-ip"
  type  = "String"
  value = aws_instance.kubernetes_server.private_ip

  tags = {
    Project = var.project_name
  }
}