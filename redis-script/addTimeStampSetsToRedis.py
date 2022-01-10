# python3 script to add jobId and timestamp to redis
import redis
import sys
from datetime import datetime, timedelta, timezone

ts1 = datetime.now().astimezone().isoformat(timespec='milliseconds').replace('+00:00', 'Z')
ts2 = (datetime.today() - timedelta(hours=0, minutes=1)).astimezone().isoformat(timespec='milliseconds').replace('+00:00', 'Z')
ts3 = (datetime.today() - timedelta(hours=0, minutes=2)).astimezone().isoformat(timespec='milliseconds').replace('+00:00', 'Z')
ts4 = (datetime.today() - timedelta(hours=0, minutes=3)).astimezone().isoformat(timespec='milliseconds').replace('+00:00', 'Z')

redisClient = redis.StrictRedis(host=str(sys.argv[1]), port=sys.argv[2], db=0, password=sys.argv[3])
redisClient.set("1", ts1)
redisClient.set("2", ts2)
redisClient.set("3", ts3)
redisClient.set("4", ts4)
