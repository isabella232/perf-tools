/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

variable "credentials_path" {
  description = "Path to the credential file"
}

variable "project_id" {
  description = "Project ID"
}

variable "default_region" {
  description = "Default Region"
  default     = "us-west1"
}

variable "default_zone" {
  description = "Default Zone"
  default     = "us-west1-a"
}

variable "cdf_vpc_subnet_cidr_range" {
  description = "CIDR range for the VPC subnet"
  default     = "10.0.0.0/20"
}

variable "database_vpc_subnet_cidr_range" {
  description = "CIDR range for the VPC subnet"
  default     = "10.10.0.0/20"
}

variable "cdf_uc1_enable_logging" {
  description = "Whether to enable Stackdriver logging for the Use Case 1 Data Fusion instance"
  default     = true
}

variable "cdf_uc1_enable_monitoring" {
  description = "Whether to enable Stackdriver monitoring for the Use Case 1 Data Fusion instance"
  default     = true
}
