resource "aws_placement_group" "listeners_ptp" {
  name     = "listeners-ptp"
  strategy = "precision-time"

  tags = {
    Name = "listeners-ptp"
  }
}