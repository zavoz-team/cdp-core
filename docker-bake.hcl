group "default" {
  targets = ["api", "worker", "migrations"]
}

target "api" {
  dockerfile = "Dockerfile"
  target     = "api"
  tags       = ["gglamer/cdp-core-api:latest"]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "worker" {
  dockerfile = "Dockerfile"
  target     = "worker"
  tags       = ["gglamer/cdp-core-worker:latest"]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "migrations" {
  dockerfile = "Dockerfile"
  target     = "migrations"
  tags       = ["gglamer/cdp-core-migrations:latest"]
  platforms  = ["linux/amd64", "linux/arm64"]
}
