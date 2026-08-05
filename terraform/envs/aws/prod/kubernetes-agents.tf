# Kubernetes agents : security group, IAM, launch template, Security Group

resource "aws_security_group" "kubernetes_agents" {
  name        = "kubernetes-agent"
  description = "kubernetes agents: SSH from control node, VXLAN between agents and to/from server"

  ingress {
    description     = "SSH from Ansible control node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_control_node_sg.id]
  }

  ingress {
    description = "Flannel VXLAN between agents"
    from_port   = 8472
    to_port     = 8472
    protocol    = "udp"
    self        = true
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "kubernetes-agents-sg"
    Project = var.project_name
  }
}

# Agent IAM: read-only access to the k3s join token in SSM
resource "aws_iam_role" "kubernetes_agent_role" {
  name = "kubernetes-agent-role"

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

resource "aws_iam_role_policy" "kubernetes_agent_ssm_policy" {
  name = "ssm-join-token-read"
  role = aws_iam_role.kubernetes_agent_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/binance-ws/*"
    }]
  })
}

resource "aws_iam_instance_profile" "kubernetes_agent_profile" {
  name = "kubernetes-agent-profile"
  role = aws_iam_role.kubernetes_agent_role.name
}

resource "aws_launch_template" "kubernetes_agent" {
  name_prefix   = "kubernetes-agent-"
  image_id      = var.ami_id
  instance_type = var.agent_instance_type
  key_name      = data.aws_key_pair.existing.key_name

  user_data = base64encode(file("${path.module}/agent-user-data.sh"))

  vpc_security_group_ids = [aws_security_group.kubernetes_agents.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.kubernetes_agent_profile.name
  }

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "kubernetes-agent"
      Project = var.project_name
      Role    = "ws-agent"
    }
  }
}

resource "aws_autoscaling_group" "kubernetes_agents" {
  name                = "kubernetes-agents"
  desired_capacity    = var.listener_count
  min_size            = var.listener_count
  max_size            = var.listener_count
  vpc_zone_identifier = [data.aws_subnet.default.id]

  launch_template {
    id      = aws_launch_template.kubernetes_agent.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "kubernetes-agent"
    propagate_at_launch = true
  }

  tag {
    key                 = "Project"
    value               = var.project_name
    propagate_at_launch = true
  }

  tag {
    key                 = "Role"
    value               = "ws-agent"
    propagate_at_launch = true
  }
}


