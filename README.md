# README #

### What is this repository for? ###

* This repository contains a simple docker-compose.yml for running Kafka in a docker container,
  together with two scripts that do the following:
  - producer.py: this script looks for images inside a folder and uploads their reference to kafka.
  - consumer.py: this script creates a PyTorch DataLoader that opens and pre-processes images whose reference was
    uploaded on kafka, then performs an inference using a DL model and saves the result images in a folder.
    It also shows the inference result live as soon as it is ready.

### How do I get set up? ###

* Install docker engine.
* Clone the repository and go to repository folder.
* Create a virtual environment. Run `virtualenv --python=/usr/bin/python<version> <path/to/new/virtualenv>`.
* Activate virtual environment. Run `source <path/to/new/virtualenv>/bin/activate`.
* Install required packages. Run `pip install -r requirements.txt`.
* Start docker daemon.
* Run `docker-compose up -d`.
* Check if kafka container does not fail (usually it fails the first time you launch it, don't exactly know why,
  just relaunch it). Got to find the cause.
* Run `python consumer.py`.
* Run `python producer.py`. It asks you for a textile identification code, insert any string you like and press Enter.
* Drag images inside the configured input folder. The producer will upload them and the consumer will perform an 
  inference on them, uploading the result in the configured output folder.
