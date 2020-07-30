FROM python:3.7-slim-buster

LABEL maintainer="areed145@gmail.com"

WORKDIR /wunderground

COPY . /wunderground

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ENV MONGODB_CLIENT 'mongodb+srv://kk6gpv:kk6gpv@cluster0-kglzh.azure.mongodb.net/test?retryWrites=true&w=majority'
ENV SID 'KTXHOUST2993'
ENV API '945638f35a724a6b9638f35a727a6bd4'

CMD ["python", "wunderground/wunderground.py"]
