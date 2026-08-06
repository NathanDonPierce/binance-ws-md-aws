# Cross-referencing security group rules

resource "aws_security_group_rule" "orchestrator_from_listeners_api" {
  type                     = "ingress"
  from_port                = 6443
  to_port                  = 6443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.orchestrator.id
  source_security_group_id = aws_security_group.listener_nodes.id
  description              = "k3s API from listener nodes"
}

resource "aws_security_group_rule" "orchestrator_from_listeners_vxlan" {
  type                     = "ingress"
  from_port                = 8472
  to_port                  = 8472
  protocol                 = "udp"
  security_group_id        = aws_security_group.orchestrator.id
  source_security_group_id = aws_security_group.listener_nodes.id
  description              = "Flannel VXLAN from listener nodes"
}

resource "aws_security_group_rule" "listeners_from_orchestrator_vxlan" {
  type                     = "ingress"
  from_port                = 8472
  to_port                  = 8472
  protocol                 = "udp"
  security_group_id        = aws_security_group.listener_nodes.id
  source_security_group_id = aws_security_group.orchestrator.id
  description              = "Flannel VXLAN from orchestrator"
}
