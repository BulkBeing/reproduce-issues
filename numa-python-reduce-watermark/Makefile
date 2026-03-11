.PHONY: image

image:
	docker build -t python-udf -f Dockerfile .
	kind load docker-image python-udf
	sleep 1
	kubectl delete po -l 'numaflow.numaproj.io/vertex-name=counter'
	kubectl delete po -l 'numaflow.numaproj.io/vertex-name=passthrough'
