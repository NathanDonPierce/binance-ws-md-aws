# Referencing key pair as defined in variables.tf
data "aws_key_pair" "existing" {
  key_name = var.key_pair_name
}

# Security group: SSH only, locked to your IP
resource "aws_security_group" "ws_client_sg" {
  name        = "ws-client-sg"
  description = "Allow SSH from my IP only"

  ingress {
    description = "SSH"
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