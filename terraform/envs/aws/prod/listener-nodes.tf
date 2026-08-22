# Listener nodes: k3s agents dedicated to running websocket listener pods.
# Three ASGs (Auto Scaling Groups) — one per Binance stream type (trade, depth, aggtrade).

variable "listener_streams" {
  description = "The set of Binance stream types each listener node group serves"
  type        = set(string)
  default     = ["trade", "depth", "aggtrade"]
}

variable "listener_count_per_stream" {
  description = "Number of listener nodes per stream type (total nodes = count × number of streams)"
  type        = number
  default     = 5
}

variable "listener_instance_type" {
  description = "EC2 instance type for listener nodes"
  type        = string
  default     = "t3.small"
}

# Shared security group for all listener nodes
resource "aws_security_group" "listener_nodes" {
  name        = "listener-nodes"
  description = "Listener nodes: SSH from ansible node, VXLAN between listeners"

  ingress {
    description     = "SSH from Ansible node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_node_sg.id]
  }

  ingress {
    description = "Flannel VXLAN between listener nodes"
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
    Name    = "listener-nodes-sg"
    Project = var.project_name
  }
}

# Shared IAM role and instance profile: read the k3s join token from SSM
resource "aws_iam_role" "listener_node_role" {
  name = "listener-node-role"

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

resource "aws_iam_role_policy" "listener_node_ssm_policy" {
  name = "ssm-join-token-read"
  role = aws_iam_role.listener_node_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/binance-ws/*"
    }]
  })
}

resource "aws_iam_instance_profile" "listener_node_profile" {
  name = "listener-node-profile"
  role = aws_iam_role.listener_node_role.name
}

# Per-stream launch template — one per stream type, differs only in the injected stream label
resource "aws_launch_template" "listener_node" {
  for_each = var.listener_streams

  name_prefix   = "listener-${each.key}-"
  image_id      = var.ami_id
  instance_type = var.listener_instance_type
  key_name      = data.aws_key_pair.existing.key_name

  vpc_security_group_ids = [aws_security_group.listener_nodes.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.listener_node_profile.name
  }

  user_data = base64encode(templatefile("${path.module}/listener-user-data.sh.tftpl", {
    stream_type = each.key
  }))

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "listener-${each.key}"
      Project = var.project_name
      Role    = "listener"
      Stream  = each.key
    }
  }
}

# Per-stream Auto Scaling Group — one per stream type
resource "aws_autoscaling_group" "listener_nodes" {
  for_each = var.listener_streams

  name                = "listener-${each.key}"
  desired_capacity    = var.listener_count_per_stream
  min_size            = var.listener_count_per_stream
  max_size            = var.listener_count_per_stream
  vpc_zone_identifier = [data.aws_subnet.default.id]

  launch_template {
    id      = aws_launch_template.listener_node[each.key].id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "listener-${each.key}"
    propagate_at_launch = true
  }

  tag {
    key                 = "Project"
    value               = var.project_name
    propagate_at_launch = true
  }

  tag {
    key                 = "Role"
    value               = "listener"
    propagate_at_launch = true
  }

  tag {
    key                 = "Stream"
    value               = each.key
    propagate_at_launch = true
  }
}
