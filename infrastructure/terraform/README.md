### Overview
Here, I provide instructions on how to reproduce the project on the infrastructure side. Please read through carefully, and make sure you have everything setup before running this.

### 1. Initial Requirements
- [x] `GCP Project Created`:
    - Service Accounted created and key is downloaded

### 2. Terraform Setup
- Please follow this link to see installation depending on your machine: [Terraform installation](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

### 3. How to run

- Navigate to directory containing `terraform` file
- In the `terminal`, run the following commands:
    - `terraform init:` This command initialize a working directory containing Terraform configuration files.
    - `terraform plan:` This command creates an execution plan, which lets you preview the changes that Terraform plans to make to your infrastructure.
        - In the terminal, you will be prompted to paste the location of your service account credentials
        - In addition, you will be required to enter your GCP Project ID
    - `terraform apply:` command executes the actions proposed in a Terraform plan.

### Note:
- `terraform init` should be run only once.
- `terraform plan` should be executed any time changes have been made to the terraform files.
- `terraform apply` should be executed to ineffect changes in the GCP.

### Resources:
- [What is Infrastructure as Code with Terraform](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/infrastructure-as-code)
- [Terraform Tutorials](https://developer.hashicorp.com/tutorials/library?product=terraform)