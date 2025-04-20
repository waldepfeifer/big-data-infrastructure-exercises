# Big Data Infrastructure Terraform Configuration

This Terraform configuration deploys the following AWS resources:

- VPC with public and private subnets
- S3 bucket for aircraft data
- RDS PostgreSQL instance
- EC2 instances for API and Airflow
- Security groups and networking components
- Elastic IPs for the EC2 instances

## Prerequisites

1. AWS CLI configured with appropriate credentials
2. Terraform installed (version >= 1.2.0)
3. SSH key pair for EC2 instances

## Usage

1. Initialize Terraform:
   ```bash
   terraform init
   ```

2. Review the planned changes:
   ```bash
   terraform plan
   ```

3. Apply the configuration:
   ```bash
   terraform apply
   ```

4. To destroy all resources:
   ```bash
   terraform destroy
   ```

## Important Notes

1. The `terraform.tfvars` file contains sensitive information. Make sure to:
   - Replace the `db_password` with a secure password
   - Add this file to `.gitignore` to prevent committing sensitive data
   - Use a secure method to manage secrets in production

2. The configuration uses:
   - Ubuntu 22.04 LTS AMI for EC2 instances
   - PostgreSQL 15.4 for RDS
   - t2.micro instances for development
   - Proper security groups with minimal access

3. Security features implemented:
   - S3 bucket encryption
   - RDS in private subnet
   - Security groups with minimal access
   - Versioning for S3 bucket
   - Backup retention for RDS

4. Monitoring and maintenance:
   - RDS backups configured
   - S3 lifecycle rules for version cleanup
   - Maintenance windows configured

## Outputs

After applying the configuration, you'll get:
- API instance public IP
- Airflow instance public IP
- RDS endpoint
- S3 bucket name

## Cost Considerations

The configuration uses:
- t2.micro instances for cost optimization
- gp2 storage for RDS
- Minimal storage allocation
- Single AZ deployment

For production, consider:
- Using larger instance types
- Multi-AZ deployment for RDS
- Increased storage allocation
- Reserved instances for cost savings 