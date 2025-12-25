# 获取Git分支名称和提交哈希值
GIT_BRANCH := $(shell git symbolic-ref --short HEAD)
GIT_COMMIT := $(shell git rev-parse --short HEAD)

# 设定镜像名称和标签
IMAGE_NAME := stock-sync
IMAGE_TAG := ${GIT_BRANCH}-${GIT_COMMIT}

# 定义build目标
build:
	docker build -f Dockerfile -t "${IMAGE_NAME}:${IMAGE_TAG}" .
	@echo "Built image ${IMAGE_NAME}:${IMAGE_TAG}"

# 定义push目标，用于将构建好的镜像推送到远程仓库
push: build
	docker push "${IMAGE_NAME}:${IMAGE_TAG}"
	@echo "Pushed image ${IMAGE_NAME}:${IMAGE_TAG} to remote repository"

## 定义deploy目标，它同时执行build和push
#deploy: build push
#	@echo "Deploy process completed for ${IMAGE_NAME}:${IMAGE_TAG}"

#.PHONY: build push deploy