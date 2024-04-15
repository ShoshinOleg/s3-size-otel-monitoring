#!/bin/bash
apt install python3-venv -y
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt