### This is the Terraform-generated header for wiley-deposits-dev. If  ###
###   this is a Lambda repo, uncomment the FUNCTION line below  ###
###   and review the other commented lines in the document.     ###
ECR_NAME_DEV:=wiley-deposits-dev
ECR_URL_DEV:=222053980223.dkr.ecr.us-east-1.amazonaws.com/wiley-deposits-dev
### End of Terraform-generated header ###

help: # preview Makefile commands
	@awk 'BEGIN { FS = ":.*#"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*#/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

### Terraform-generated Developer Deploy Commands for Dev environment ###
dist-dev: ## Build docker container (intended for developer-based manual build)
	docker build --platform linux/amd64 \
	    -t $(ECR_URL_DEV):latest \
		-t $(ECR_URL_DEV):`git describe --always` \
		-t $(ECR_NAME_DEV):latest .

publish-dev: dist-dev ## Build, tag and push (intended for developer-based manual publish)
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL_DEV)
	docker push $(ECR_URL_DEV):latest
	docker push $(ECR_URL_DEV):`git describe --always`

### Terraform-generated manual shortcuts for deploying to Stage. This requires  ###
###   that ECR_NAME_STAGE, ECR_URL_STAGE, and FUNCTION_STAGE environment        ###
###   variables are set locally by the developer and that the developer has     ###
###   authenticated to the correct AWS Account. The values for the environment  ###
###   variables can be found in the stage_build.yml caller workflow.            ###
dist-stage: ## Only use in an emergency
	docker build --platform linux/amd64 \
	    -t $(ECR_URL_STAGE):latest \
		-t $(ECR_URL_STAGE):`git describe --always` \
		-t $(ECR_NAME_STAGE):latest .

publish-stage: ## Only use in an emergency
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL_STAGE)
	docker push $(ECR_URL_STAGE):latest
	docker push $(ECR_URL_STAGE):`git describe --always`

run-deposit-dev: ## Run 'deposit' ECS task in Dev1
	aws ecs run-task --cluster DSS-wiley-dev --task-definition DSS-wiley-dev --network-configuration "awsvpcConfiguration={subnets=[subnet-0488e4996ddc8365b,subnet-022e9ea19f5f93e65],securityGroups=[sg-044033bf5f102c544],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-dev: ## Run 'listen' ECS task in Dev1
	aws ecs run-task --cluster DSS-wiley-dev --task-definition DSS-wiley-dev --network-configuration "awsvpcConfiguration={subnets=[subnet-0488e4996ddc8365b,subnet-022e9ea19f5f93e65],securityGroups=[sg-044033bf5f102c544],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'

run-deposit-stage: ## Run 'deposit' ECS task in Stage-Workloads
	aws ecs run-task --cluster DSS-wiley-stage --task-definition DSS-wiley-stage --network-configuration "awsvpcConfiguration={subnets=[subnet-05df31ac28dd1a4b0,subnet-04cfa272d4f41dc8a],securityGroups=[sg-0f64d9a1101d544d1],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-stage: ## Run 'listen' ECS task in Stage-Workloads
	aws ecs run-task --cluster DSS-wiley-stage --task-definition DSS-wiley-stage --network-configuration "awsvpcConfiguration={subnets=[subnet-05df31ac28dd1a4b0,subnet-04cfa272d4f41dc8a],securityGroups=[sg-0f64d9a1101d544d1],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'

run-deposit-prod: ## Run 'deposit' ECS task in Prod-Workloads
	aws ecs run-task --cluster DSS-wiley-prod --task-definition DSS-wiley-prod --network-configuration "awsvpcConfiguration={subnets=[subnet-042726f373a7c5a79,subnet-05ab0e5c2bfcd748f],securityGroups=[sg-0325d8c490a870a90],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-prod: ## Run 'listen' ECS task in Prod-Workloads
	aws ecs run-task --cluster DSS-wiley-prod --task-definition DSS-wiley-prod --network-configuration "awsvpcConfiguration={subnets=[subnet-042726f373a7c5a79,subnet-05ab0e5c2bfcd748f],securityGroups=[sg-0325d8c490a870a90],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'

#######################
# Dependency commands
#######################

install: # Install Python dependencies and pre-commit hook
	pipenv install --dev
	pipenv run pre-commit install

update: install # Update all Python dependencies
	pipenv clean
	pipenv update --dev

######################
# Unit test commands 
######################

test: # Run tests and print a coverage report
	pipenv run coverage run --source=awd -m pytest -vv
	pipenv run coverage report -m

coveralls: test
	pipenv run coverage lcov -o ./coverage/lcov.info

####################################
# Code quality and safety commands
####################################

lint: black mypy ruff safety # Run linters

black: # Run 'black' linter and print a preview of suggested changes
	pipenv run black --check --diff .

mypy: # Run 'mypy' linter
	pipenv run mypy .

ruff: # Run 'ruff' linter and print a preview of errors
	pipenv run ruff check .

safety: # Check for security vulnerabilities and verify Pipfile.lock is up-to-date
	pipenv check
	pipenv verify

lint-apply: # Apply changes with 'black' and resolve fixable errors with 'ruff'
	black-apply ruff-apply

black-apply: # Apply changes with 'black'
	pipenv run black .

ruff-apply: # Resolve fixable errors with 'ruff'
	pipenv run ruff check --fix .