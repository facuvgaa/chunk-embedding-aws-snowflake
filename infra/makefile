build:
	docker compose up -d --build

down:
	docker compose down

sh:
	docker exec -it terraform_infra bash

terraform-init:
	docker exec -it terraform_infra bash -lc "cd /infra && terraform init"

terraform-validate:
	docker exec -it terraform_infra bash -lc "cd /infra && terraform validate"

terraform-plan:
	docker exec -it terraform_infra bash -lc "cd /infra && terraform plan -out=tfplan"

terraform-apply:
	docker exec -it terraform_infra bash -lc "cd /infra && (terraform apply -auto-approve tfplan || terraform apply -auto-approve)"

terraform-destroy:
	docker exec -it terraform_infra bash -lc "cd /infra && terraform destroy -auto-approve"