FROM python:3.7-alpine

COPY . /opt/amazon
WORKDIR /opt/amazon
ENV PATH="/opt/amazon/bin:${PATH}"
ENV PYTHONPATH="/opt/amazon:${PYTHONPATH}"
RUN pip install -r requirements.txt
