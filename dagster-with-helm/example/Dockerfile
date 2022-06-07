FROM python:3.10-alpine

ARG PROJECT=example-project

ENV TZ=UTC

WORKDIR /${PROJECT}

COPY . /${PROJECT}

RUN python -m pip install -r requirements.txt
