import redis

r = redis.Redis(host='localhost', port=6379)

r.set('key_ex', 'data')

value = r.get('key_ex')
print(value)

r.delete('key_ex')
