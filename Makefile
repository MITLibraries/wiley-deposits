SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
ECR_REGISTRY=672626379771.dkr.ecr.us-east-1.amazonaws.com

dist: ## Build docker container
	docker build --platform linux/amd64 -t $(ECR_REGISTRY)/wiley-deposits-stage:latest \
		-t $(ECR_REGISTRY)/wiley-deposits-stage:`git describe --always` \
		-t awd:latest .	

publish: dist ## Build, tag and push
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_REGISTRY)
	docker push $(ECR_REGISTRY)/wiley-deposits-stage:latest
	docker push $(ECR_REGISTRY)/wiley-deposits-stage:`git describe --always`

promote: ## Promote the current staging build to production
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_REGISTRY)
	docker pull $(ECR_REGISTRY)/wiley-deposits-stage:latest
	docker tag $(ECR_REGISTRY)/wiley-deposits-stage:latest $(ECR_REGISTRY)/wiley-deposits-prod:latest
	docker tag $(ECR_REGISTRY)/wiley-deposits-stage:latest $(ECR_REGISTRY)/wiley-deposits-prod:$(DATETIME)
	docker push $(ECR_REGISTRY)/wiley-deposits-prod:latest
	docker push $(ECR_REGISTRY)/wiley-deposits-prod:$(DATETIME)

check-permissions-stage: ## Check infrastructure permissions on the staging deplpyment
	aws ecs run-task --cluster wiley-stage --task-definition wiley-stage --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-051bad317b4a14803],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["check-permissions"]}]}'

run-deposit-stage: ## Run the stage-deposit command
	aws ecs run-task --cluster wiley-stage --task-definition wiley-stage --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-051bad317b4a14803],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-stage: ## Run the stage listen command
	aws ecs run-task --cluster wiley-stage --task-definition wiley-stage --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-051bad317b4a14803],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'

check-permissions-prod: ## Check infrastructure permissions on the prod deplpyment
	aws ecs run-task --cluster wiley-prod --task-definition wiley-prod --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-0f3730fd8f7ade474],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["check-permissions"]}]}'

run-deposit-prod: ## Run the prod deposit command
	aws ecs run-task --cluster wiley-prod --task-definition wiley-prod --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-0f3730fd8f7ade474],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-prod: ## Run the prod listen command
	aws ecs run-task --cluster wiley-prod --task-definition wiley-prod --network-configuration "awsvpcConfiguration={subnets=[subnet-0744a5c9beeb49a20],securityGroups=[sg-0f3730fd8f7ade474],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'
	
lint: bandit black flake8 isort

bandit:
	pipenv run bandit -r awd

black:
	pipenv run black --check --diff .
	
coveralls: test
	pipenv run coveralls

flake8:
	pipenv run flake8 .

isort:
	pipenv run isort . --diff
	
test:
	pipenv run pytest --cov=awd
