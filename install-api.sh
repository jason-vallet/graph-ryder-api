#!/bin/sh

#docker run -it --name api -v "$PWD":/usr/src/myapp -w /usr/src/myapp -p 5000:5000 python:3 bash

pip install -r requirements.txt
cp exportsigma.py /usr/local/lib/python3.5/site-packages/tulip/native/plugins/exportsigma.py
python app.py
