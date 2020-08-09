FROM python:3.7-alpine

RUN apk update
RUN echo "http://dl-8.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories
RUN apk --no-cache --update-cache add gcc gfortran build-base wget freetype-dev libpng-dev openblas-dev
RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

COPY . /opt/amazon
WORKDIR /opt/amazon
ENV PATH="/opt/amazon/bin:${PATH}"
ENV PYTHONPATH="/opt/amazon:${PYTHONPATH}"
RUN pip install --no-cache-dir -r requirements.txt
