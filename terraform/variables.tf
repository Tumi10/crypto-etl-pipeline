variable "region"{ 
    type          = string 
    description   = " Azure Region" 
    default       = "eastus "
} 

variable "resource_group_name"{
    type        = string 
    description = "Resource Group Name"
    default     = "crypto-rg"
}

variable "cluster_name"{
    type = string 
    description = "Cluster_Name "
    default     = "crypto-aks"
}