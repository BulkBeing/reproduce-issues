IMAGE_NAME := all-udfs
VERSION_FILE := .image-version
# Docker build context is the pynumaflow package root (2 levels up)
BUILD_CONTEXT := ../..

# Read current version, default to 0 if file doesn't exist
CURRENT_VERSION := $(shell cat $(VERSION_FILE) 2>/dev/null || echo 0)
NEXT_VERSION := $(shell echo $$(( $(CURRENT_VERSION) + 1 )))
TAG := v$(NEXT_VERSION)
IMAGE := $(IMAGE_NAME):$(TAG)

.PHONY: image

image:
	@echo "==> Building $(IMAGE)"
	docker build -f Dockerfile -t $(IMAGE) $(BUILD_CONTEXT)
	@echo $(NEXT_VERSION) > $(VERSION_FILE)
	@echo "==> Loading $(IMAGE) into kind"
	kind load docker-image $(IMAGE)
	@echo "==> Updating pipeline.yaml to $(IMAGE)"
	sed -i 's|image: $(IMAGE_NAME):v[0-9]*|image: $(IMAGE_NAME):$(TAG)|g' pipeline.yaml
	@echo "==> Applying pipeline"
	kubectl apply -f pipeline.yaml
	@echo "==> Done - deployed $(IMAGE)"
