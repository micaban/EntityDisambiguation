FROM ubuntu:20.04

RUN apt-get update -y
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y git

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt
RUN pip install --user git+https://github.com/technige/py2neo.git@py2neo-3.1.2

COPY . .

#WORKDIR /app


CMD ["python3", "microservice.py"]

#ENV PATH="/home/worker/.local/bin:${PATH}"

###### USER COMMANDS
#USER worker

#COPY requirements.txt /requirements.txt


#RUN pip install --user -r /requirements.txt

#COPY . ./app

#WORKDIR  /home/worker/app

#CMD ["python3", "-u", "microservice.py"]