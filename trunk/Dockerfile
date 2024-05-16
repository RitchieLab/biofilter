FROM python:3.11-alpine

ARG BIOFILTER_VERSION=2.4.3
RUN apk add --no-cache wget tar bash

RUN addgroup -S biofilter && adduser -S biofilter -G biofilter

WORKDIR /app

RUN pip install --no-cache-dir apsw wget

RUN wget https://github.com/RitchieLab/biofilter/releases/download/Releases/biofilter-${BIOFILTER_VERSION}.tar.gz -O biofilter.tar.gz
RUN mkdir /app/biofilter
RUN tar -zxvf biofilter.tar.gz --strip-components=1 -C /app/biofilter
WORKDIR /app/biofilter
RUN python setup.py install

RUN chown -R biofilter:biofilter /app
RUN chmod -R 777 /app

USER biofilter