### This is the Terraform-generated header for wiley-deposits-dev. If  ###
###   this is a Lambda repo, uncomment the FUNCTION line below  ###
###   and review the other commented lines in the document.     ###
ECR_NAME_DEV:=wiley-deposits-dev
ECR_URL_DEV:=222053980223.dkr.ecr.us-east-1.amazonaws.com/wiley-deposits-dev
# FUNCTION_DEV:=
### End of Terraform-generated header                            ###

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

### If this is a Lambda repo, uncomment the two lines below     ###
# update-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr (intended for developer-based manual update)
#	aws lambda update-function-code --function-name $(FUNCTION_DEV) --image-uri $(ECR_URL_DEV):latest


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

### If this is a Lambda repo, uncomment the two lines below     ###
# update-lambda-stage: ## Updates the lambda with whatever is the most recent image in the ecr (intended for developer-based manual update)
#	aws lambda update-function-code --function-name $(FUNCTION_STAGE) --image-uri $(ECR_URL_STAGE):latest


run-deposit-dev: ## Run the dev deposit command
	aws ecs run-task --cluster DSS-wiley-dev --task-definition DSS-wiley-dev --network-configuration "awsvpcConfiguration={subnets=[subnet-0488e4996ddc8365b,subnet-022e9ea19f5f93e65],securityGroups=[sg-044033bf5f102c544],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["deposit"]}]}'

run-listen-dev: ## Run the dev listen command
	aws ecs run-task --cluster DSS-wiley-dev --task-definition DSS-wiley-dev --network-configuration "awsvpcConfiguration={subnets=[subnet-0488e4996ddc8365b,subnet-022e9ea19f5f93e65],securityGroups=[sg-044033bf5f102c544],assignPublicIp=DISABLED}" --launch-type FARGATE --region us-east-1 --overrides '{"containerOverrides": [{"name": "wiley","command": ["listen"]}]}'


### Dependency commands ###
install: ## Install script and dependencies
	pipenv install --dev

update: install ## Update all Python dependencies
	pipenv clean
	pipenv update --dev


### Testing commands ###
test: ## Run tests and print a coverage report
	pipenv run coverage run --source=awd -m pytest -vv
	pipenv run coverage report -m

coveralls: test
	pipenv run coverage lcov -o ./coverage/lcov.info


### Linting commands ###
lint: black mypy safety

black:
	pipenv run black --check --diff .

mypy:
	pipenv run mypy awd

safety:
	pipenv check
	pipenv verify

# Apply changes to resolve any linting errors
lint-apply: black-apply

black-apply: 
	pipenv run black .
