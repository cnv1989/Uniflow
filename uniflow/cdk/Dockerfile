FROM python:3.7-alpine

ENV PATH "/usr/local/bin:/opt/amazon/bin:${PATH}"

COPY . /opt/amazon
WORKDIR /opt/amazon
RUN ls
RUN pip install -r requirements.txt






