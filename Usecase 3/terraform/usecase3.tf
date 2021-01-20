# VPC network for the CDF instance
resource "google_compute_network" "cdf_benchmarking_vpc" {
  name                    = "cdf-benchmarking--vpc"
  auto_create_subnetworks = false
  project                 = var.project_id
}

# Subnet for the CDF VPC Network
resource "google_compute_subnetwork" "cdf_benchmarking_subnet" {
  name          = "cdf-benchmarking-vpc-subnet1"
  project       = var.project_id
  ip_cidr_range = var.cdf_vpc_subnet_cidr_range
  region        = var.default_region
  network       = google_compute_network.cdf_benchmarking_vpc.name
}

# Private CDF instance for Use Case 3
resource "google_data_fusion_instance" "cdf_instance_uc3" {
  provider                      = google-beta
  name                          = "cdf-benchmarking-uc3"
  project                       = var.project_id
  region                        = var.default_region
  type                          = "ENTERPRISE"
  version                       = "6.2.2"
  private_instance              = true
  enable_stackdriver_logging    = var.cdf_uc3_enable_logging
  enable_stackdriver_monitoring = var.cdf_uc3_enable_monitoring

  network_config {
    ip_allocation = "${google_compute_global_address.private_ip_alloc.address}/${google_compute_global_address
                    .private_ip_alloc.prefix_length}"
    network       = google_compute_network.cdf_benchmarking_vpc.name
  }
}

# Allocates a private IP range for the private CDF instance
resource "google_compute_global_address" "private_ip_alloc" {
  name          = "cdf-benchmarking-private-ip-range"
  project       = var.project_id
  network       = google_compute_network.cdf_benchmarking_vpc.id
  address_type  = "INTERNAL"
  purpose       = "VPC_PEERING"
  prefix_length = 22
}

# Peering connection between private CDF instance VPC and the Tenant project VPC
resource "google_compute_network_peering" "cdf_tenant_peering" {
  name                 = "cdf-benchmarking-cdf-to-tenant"
  network              = google_compute_network.cdf_benchmarking_vpc.id
  # uses the regex function to extract the private CDF tenant project id
  peer_network         = "projects/${element(regex("@([a-zA-Z0-9-]+).", google_data_fusion_instance.cdf_instance_uc3
                         .service_account), 0)}/global/networks/${var.default_region}-${google_data_fusion_instance
                         .cdf_instance_uc3.name}"
  export_custom_routes = true
  import_custom_routes = true
}

# Allows traffic to port 22 for the Benchmarking VPC
resource "google_compute_firewall" "ssh_firewall" {
  name     = "cdf-benchmarking-vpc-allow-ssh"
  project  = var.project_id
  network  = google_compute_network.cdf_benchmarking_vpc.name
  priority = 100

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Static IP address for SQL Server VM
resource "google_compute_address" "sqlserver_static_ip_uc3" {
  name    = "sql-server-uc3-ip"
  project = var.project_id
  region  = var.default_region
}

# Compute Engine VM with SQL Server pre-installed using a startup script
resource "google_compute_instance" "sqlserver_vm_uc3" {
  name                    = "sql-server-uc3"
  project                 = var.project_id
  zone                    = var.default_zone
  machine_type            = "n2-standard-32"
  metadata_startup_script = file("./installation-scripts/install-sql-server.sh")

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
      size  = "8000"
      type  = "pd-ssd"
    }
  }

  network_interface {
    network    = google_compute_network.cdf_benchmarking_vpc.name
    subnetwork = google_compute_subnetwork.cdf_benchmarking_subnet.name
    access_config {
      nat_ip = google_compute_address.sqlserver_static_ip_uc3.address
    }
  }
}

# Compute Engine VM for the Pub/Sub publisher. Upload the publisher JAR file
# after building the pubsub-publisher project to publish messages
resource "google_compute_instance" "pubsub_vm" {
  name         = "pubsub-publisher-uc3"
  project      = var.project_id
  zone         = var.default_zone
  machine_type = "n2-standard-8"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
      size  = "500"
      type  = "pd-ssd"
    }
  }

  network_interface {
    network    = google_compute_network.cdf_benchmarking_vpc.name
    subnetwork = google_compute_subnetwork.cdf_benchmarking_subnet.name
    access_config {
      # Ephemeral IP
    }
  }
}

# Pub/Sub topic
resource "google_pubsub_topic" "pubsub_topic_uc3" {
  name = "cdf-benchmarking-topic-uc3"
}

# Pub/Sub subscription for the topic created above
resource "google_pubsub_subscription" "pubsub_subscription_uc3" {
  name                       = "cdf-benchmarking-sub-uc3"
  topic                      = google_pubsub_topic.pubsub_topic_uc3.name
  message_retention_duration = "432000s"   # 5 days
  retain_acked_messages      = false
  ack_deadline_seconds       = 60
}

# Random string to append to the gcs bucket
resource "random_string" "random" {
  length  = 5
  special = false
  number  = true
  upper   = false
}

# GCS Bucket
resource "google_storage_bucket" "gcs_uc3" {
  name          = "cdf-benchmarking-uc3-${random_string.random.id}"
  storage_class = "STANDARD"
  location      = var.default_region
  force_destroy = true    # deletes all objects in the bucket if 'terraform destroy' is run
}
