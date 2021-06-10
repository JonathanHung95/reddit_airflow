#!/bin/sh

# potentially will need a bootstrap script to get the worker nodes to install the correct nltk packages

sudo pip install nltk
sudo pip install -m nltk.downloader -d /home/nltk_data averaged_perceptron_tagger