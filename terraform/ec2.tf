# Key Pair
resource "aws_key_pair" "main" {
  key_name   = "bdi-key-pair"
  public_key = file("~/.ssh/id_rsa.pub")

  tags = merge(local.common_tags, {
    Name = "bdi-key-pair"
  })
}

# API Instance
resource "aws_instance" "api" {
  ami                    = "ami-0c7217cdde317cfec" # Ubuntu 22.04 LTS
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.api.id]
  key_name               = aws_key_pair.main.key_name

  root_block_device {
    volume_size = 8
    volume_type = "gp2"
  }

  tags = merge(local.common_tags, {
    Name = "bdi-api-instance"
  })

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y docker.io
              systemctl start docker
              systemctl enable docker
              EOF
}

# Airflow Instance
resource "aws_instance" "airflow" {
  ami                    = "ami-0c7217cdde317cfec" # Ubuntu 22.04 LTS
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.airflow.id]
  key_name               = aws_key_pair.main.key_name

  root_block_device {
    volume_size = 8
    volume_type = "gp2"
  }

  tags = merge(local.common_tags, {
    Name = "bdi-airflow-instance"
  })

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y docker.io
              systemctl start docker
              systemctl enable docker
              EOF
}

# Elastic IP for API Instance
resource "aws_eip" "api" {
  instance = aws_instance.api.id
  vpc      = true

  tags = merge(local.common_tags, {
    Name = "bdi-api-eip"
  })
}

# Elastic IP for Airflow Instance
resource "aws_eip" "airflow" {
  instance = aws_instance.airflow.id
  vpc      = true

  tags = merge(local.common_tags, {
    Name = "bdi-airflow-eip"
  })
} 