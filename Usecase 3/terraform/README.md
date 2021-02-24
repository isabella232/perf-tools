# CDF Benchmarking Use Case 3 Environment Setup

![UC 1 Architecture Diagram](uc3-architecture-diagram.png)

This Terraform script can be used to spin up the GCP resources required for benchmarking the following 
Use Case 3 pipelines:
- Data Generator to GCS
- Data Generator to BigQuery
- Pub/Sub to Trash
- Pub/Sub to BigQuery
- GCS to Trash
- GCS to GCS
- GCS to BigQuery
- GCS to BigQuery with simulated latency
- BigQuery to Trash
- Multiple SQL Server tables to multiple BigQuery tables

Note that the actual pipelines need to be run using the python automation scripts that leverage the
[CDAP REST API](https://cloud.google.com/data-fusion/docs/reference/cdap-reference).


## List of Resources

- One (1) Private Cloud Data Fusion Enterprise Instance
- One (1) Static IP address for the SQL Server VM
- Two (2) Compute Engine VMs (SQL Server and Pub/Sub Publisher)
- One (1) Private IP range for Cloud Data Fusion
- One (1) VPC Network for the private Cloud Data Fusion instance and the SQL Server VM
- One (1) Subnet under the Cloud Data Fusion VPC Network
- One (1) Firewall Rule to allow traffic to TCP port 22 (to SSH into the SQL Server VM)
- One (1) Pub/Sub Topic
- One (1) Pub/Sub Subscription
- One (1) GCS Bucket
- One way Peering connection between the private Cloud Data Fusion VPC network and the VPC network in the tenant project


## Prerequisites

### Install Terraform
Install Terraform by following the steps mentioned in documentation
[here](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/gcp-get-started)

### Service Account
Create a service account with the following permissions
- Compute Admin
- Cloud Data Fusion Admin
- Storage Admin
- Pub/Sub Admin

### Variables 
Create a *terraform.tfvars* file in the terraform root directory with the following variables
```
credentials_path = [path/to/service-account-key.json]
project_id       = [project_id]
```

The following variables can also be set. Default values in the *variables.tf* file will be picked up if not present in
the *terraform.tfvars* file
```
default_region                 = "us-west1"
default_zone                   = "us-west1-a"
cdf_vpc_subnet_cidr_range      = "10.0.0.0/20"
database_vpc_subnet_cidr_range = "10.10.0.0/20"
cdf_uc3_enable_logging         = true
cdf_uc3_enable_monitoring      = true
```


## Creating the resources

To provision the resources, run the following from within this directory    
- `terraform init` to prepare the current working directory for use with Terraform  
- `terraform plan` to get a list of the resources that would be created
- `terraform apply` to create the resources


## Destroying the resources

The resources can be deleted after running the desired benchmarking pipelines by running `terraform destroy`
