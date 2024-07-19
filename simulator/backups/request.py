import json
import random

from uuid import uuid4

class Request:
    def __init__(self, model='A', input_length=4, is_swap=False, id=None):
        self.model = model
        self.input_length = input_length
        self.is_swap = is_swap
        self.id = str(uuid4())[:8] if id is None else id

    def __str__(self):
        return json.dumps({
            'model': self.model,
            'input_length': self.input_length,
            'is_swap': self.is_swap,
            'id': self.id
        })
    
def request_from_str(msg):
    msg = json.loads(msg)
    return Request(model=msg['model'], input_length=msg['input_length'], is_swap=msg['is_swap'], id=msg['id'])

def random_request(models):
    return Request(model=random.choice(models), input_length=random.randint(10, 100))