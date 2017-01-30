FROM python:3-onbuild
EXPOSE 5000
RUN pip install -r requirements.txt
COPY exportsigma.py /usr/local/lib/python3.5/site-packages/tulip/native/plugins/exportsigma.py
CMD [ "python", "./app.py" ]
