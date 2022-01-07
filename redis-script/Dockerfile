FROM python:3

ENV REDISHOST=localhost
ENV REDISPORT=6379
ENV REDISPASSWORD=""

RUN mkdir /app
ADD . /app
WORKDIR /app

RUN pip install --no-cache-dir -r installables.txt

ENTRYPOINT python3 addTimeStampSetsToRedis.py $REDISHOST $REDISPORT $REDISPASSWORD