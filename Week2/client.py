import redis

r = redis.Redis(host='localhost', port=6379)

for i in range(100):
    value = r.rpop("data")
    print("Value {:} is got in the {:}th place".format(int(value), i))

