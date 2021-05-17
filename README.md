python3 -m venv venv_kafka_using_python

pip install kafka-python


docker container stop kafka3

docker container start kafka3

docker container logs kafka3
docker container logs kafka2

docker container exec kafka2 -it bash