variable "function_name" { type = string }
variable "zip_path" {
  type    = string
  default = "lambda_stub.zip"
}
variable "tags" { type = map(string) }
