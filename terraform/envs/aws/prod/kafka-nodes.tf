# Kafka nodes: k3s agents dedicated to running Kafka broker pods.
# Tainted role=kafka so listener workloads never land here; labelled role=kafka
# so Strimzi can target them. Managed by an ASG so a lost node is replaced
# automatically and its broker pod reschedules onto the replacement.

variable "kafka_node_count" {
  description = "Number of Kafka broker nodes (one broker pod each)"
  type        = number
  default     = 3
}

variable "kafka_instance_type" {
  description = "EC2 instance type for Kafka broker nodes"
  type        = string
  default     = "c7i-flex.large"
}

resource "aws_security_group" "kafka_nodes" {
  name        = "kafka-nodes"
  description = "Kafka nodes: SSH from control node, VXLAN between cluster nodes, Kafka broker traffic"

  ingress {
    description     = "SSH from Ansible control node"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.ansible_node_sg.id]
  }

  ingress {
    description = "Flannel VXLAN between Kafka nodes"
    from_port   = 8472
    to_port     = 8472
    protocol    = "udp"
    self        = true
  }

  ingress {
    description = "Kafka broker traffic between Kafka nodes (inter-broker replication)"
    from_port   = 9090
    to_port     = 9093
    protocol    = "tcp"
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
    Name    = "kafka-nodes-sg"
    Project = var.project_name
  }
}

# Kafka node IAM: read-only access to the k3s join token in SSM (same as listeners)
resource "aws_iam_role" "kafka_node_role" {
  name = "kafka-node-role"

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

resource "aws_iam_role_policy" "kafka_node_ssm_policy" {
  name = "ssm-join-token-read"
  role = aws_iam_role.kafka_node_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/binance-ws/*"
    }]
  })
}

resource "aws_iam_instance_profile" "kafka_node_profile" {
  name = "kafka-node-profile"
  role = aws_iam_role.kafka_node_role.name
}

resource "aws_launch_template" "kafka_node" {
  name_prefix   = "kafka-node-"
  image_id      = var.ami_id
  instance_type = var.kafka_instance_type
  key_name      = data.aws_key_pair.existing.key_name

  vpc_security_group_ids = [aws_security_group.kafka_nodes.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.kafka_node_profile.name
  }

  user_data = base64encode(file("${path.module}/kafka-node-user-data.sh"))

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "kafka-node"
      Project = var.project_name
      Role    = "kafka"
    }
  }
}

resource "aws_autoscaling_group" "kafka_nodes" {
  name                = "kafka-nodes"
  desired_capacity    = var.kafka_node_count
  min_size            = var.kafka_node_count
  max_size            = var.kafka_node_count
  vpc_zone_identifier = [data.aws_subnet.default.id]

  launch_template {
    id      = aws_launch_template.kafka_node.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "kafka-node"
    propagate_at_launch = true
  }

  tag {
    key                 = "Project"
    value               = var.project_name
    propagate_at_launch = true
  }

  tag {
    key                 = "Role"
    value               = "kafka"
    propagate_at_launch = true
  }
}

