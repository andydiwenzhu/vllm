import redis

class MessageQueue:
    def __init__(self, queue_name, redis_client=redis.Redis(host='localhost', port=6379, db=0)):
        self.redis_client = redis_client
        self.queue_name = 'MQ-' + queue_name
    def push(self, message):
        pass
    def pop(self):
        pass
    