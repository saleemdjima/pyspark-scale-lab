variable "region" {
    description = "Région AWS unique pour S3 + Glue + Athena"
    type = string 
    default = "eu-central-1"
}

variable "project" {
    description = "Préfixe de nommage des ressources" 
    type  = string 
    default = "market-lab"
}