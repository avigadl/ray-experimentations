
sudo docker build -t ray-jupyter-aarch64:latest .

docker tag ray-jupyter-aarch64:latest avigadl/ray-jupyter-aarch64:latest

docker push avigadl/ray-jupyter-aarch64:latest