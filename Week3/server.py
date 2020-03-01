import redis

r = redis.Redis(host='localhost', port=6379)

for i in range(100):
    r.lpush("data", i)


