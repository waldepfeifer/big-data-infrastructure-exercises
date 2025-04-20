# S3 Bucket
resource "aws_s3_bucket" "aircraft_data" {
  bucket = "bdi-aircraft-waldepfeifer"

  tags = merge(local.common_tags, {
    Name = "bdi-aircraft-data"
  })
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "aircraft_data" {
  bucket = aws_s3_bucket.aircraft_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Server Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "aircraft_data" {
  bucket = aws_s3_bucket.aircraft_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "aircraft_data" {
  bucket = aws_s3_bucket.aircraft_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Lifecycle Rule
resource "aws_s3_bucket_lifecycle_configuration" "aircraft_data" {
  bucket = aws_s3_bucket.aircraft_data.id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
} 