import redis

from request import request_from_str
class MessageQueue:
    def __init__(self, queue_name, redis_client=redis.Redis(host='localhost', port=6379, db=0)):
        self.redis_client = redis_client
        self.queue_name = 'MQ-' + queue_name
    def push(self, request):
        self.redis_client.rpush(self.queue_name, str(request))
    def pop(self):
        result = self.redis_client.lpop(self.queue_name)
        return request_from_str(result.decode('utf-8')) if result else None
    