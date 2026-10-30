variable "project" {
  description = "Name of the project, in the Project tag of every resource and at the start of their names"
  type        = string
  default     = "sedona-demo"
}

variable "region" {
  description = "Region of the cluster and the bucket. GBIF has a copy of its snapshot in us-east-1"
  type        = string
  default     = "us-east-1"
}

variable "gbif_snapshot" {
  description = "Monthly snapshot of GBIF on S3, as the date of its folder"
  type        = string
  default     = "2026-10-01"
}

variable "overture_release" {
  description = "Release of Overture Maps on S3. Overture only keeps its latest releases"
  type        = string
  default     = "2026-09-23.1"
}

variable "vpc_cidr" {
  description = "Address range of the VPC of the project"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "Address range of the public subnet of the VPC, where the cluster runs"
  type        = string
  default     = "10.0.0.0/24"
}

variable "release_label" {
  description = "EMR release, which sets the versions of Spark, Python and the rest"
  type        = string
  default     = "emr-spark-8.1.0"
}

variable "primary_instance_type" {
  description = "Instance type of the primary node, which runs YARN, HDFS and the Spark driver"
  type        = string
  default     = "m7g.2xlarge"
}

variable "worker_instance_type" {
  description = "Instance type of the core and task nodes, which run the Spark executors"
  type        = string
  default     = "c7g.8xlarge"
}

variable "core_count" {
  description = "Number of core nodes, on demand"
  type        = number
  default     = 1
}

variable "task_count" {
  description = "Number of task nodes"
  type        = number
  default     = 5
}

variable "task_on_spot" {
  description = "Whether the task nodes are spot instances, paying at most the on-demand price, or on demand"
  type        = bool
  default     = true
}

variable "idle_timeout_seconds" {
  description = "The cluster terminates itself after being idle for this long"
  type        = number
  default     = 1800
}

variable "driver_memory" {
  description = "Memory of the Spark driver, which runs on the primary node"
  type        = string
  default     = "16g"
}

variable "network_tag" {
  description = "Value of the Module tag on the network resources"
  type        = string
  default     = "network"
}

variable "storage_tag" {
  description = "Value of the Module tag on the storage resources"
  type        = string
  default     = "storage"
}

variable "catalog_tag" {
  description = "Value of the Module tag on the data catalog resources"
  type        = string
  default     = "catalog"
}

variable "compute_tag" {
  description = "Value of the Module tag on the compute resources"
  type        = string
  default     = "compute"
}

variable "query_tag" {
  description = "Value of the Module tag on the query resources"
  type        = string
  default     = "query"
}
