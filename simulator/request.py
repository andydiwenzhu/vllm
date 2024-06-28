import random

from uuid import uuid4

class Request:
    def __init__(self, model, input_length, is_swap=False):
        self.model = model
        self.input_length = input_length
        self.is_swap = is_swap
        self.id = str(uuid4())

    def __init__(self, s):
        pass

def random_request(models):
    return Request(random.choice(models), random.randint(10, 1024))