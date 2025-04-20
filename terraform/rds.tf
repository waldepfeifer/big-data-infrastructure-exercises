# RDS Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "bdi-rds-subnet-group"
  subnet_ids = [aws_subnet.private.id]

  tags = merge(local.common_tags, {
    Name = "bdi-rds-subnet-group"
  })
}

# RDS Parameter Group
resource "aws_db_parameter_group" "main" {
  name   = "bdi-postgres-params"
  family = "postgres15"

  parameter {
    name  = "log_statement"
    value = "all"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }

  tags = merge(local.common_tags, {
    Name = "bdi-postgres-params"
  })
}

# RDS Instance
resource "aws_db_instance" "main" {
  identifier           = "bds"
  engine              = "postgres"
  engine_version      = "15.4"
  instance_class      = "db.t4g.micro"
  allocated_storage   = 8
  storage_type        = "gp2"
  
  db_name             = "bds"
  username            = "my_bds_user"
  password            = var.db_password
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  parameter_group_name   = aws_db_parameter_group.main.name
  
  backup_retention_period = 1
  backup_window          = "07:01-07:31"
  maintenance_window     = "Mon:00:00-Mon:03:00"
  
  skip_final_snapshot    = false
  final_snapshot_identifier = "bds-final-snapshot"
  
  tags = merge(local.common_tags, {
    Name = "bdi-rds-instance"
  })
} 