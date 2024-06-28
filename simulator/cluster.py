import random
import redis
import threading
import time

from lock import DistributedLock
from message_queue import MessageQueue
from request import Request, random_request

LOCK_KEY = 'global-instance-lock'
BATCH_SIZE_LIMIT = 5    

class Gateway:
    def __init__(self, api_servers):
        self.api_servers = api_servers
        self.cnt = 0
    def on_request(self, request):
        self.api_servers[self.cnt % len(self.api_servers)].on_request(request)
        self.cnt += 1

class ApiServer:
    def __init__(self, scheduler_id):
        self.mq = MessageQueue(scheduler_id)

    def on_request(self, request):
        self.mq.push(request)

class Scheduler:
    def __init__(self, id):
        self.id = id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.mq = MessageQueue(id, self.redis_client)
        self.lock = DistributedLock(LOCK_KEY, self.redis_client)
        self._stop_event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        while not self._stop_event.is_set():
            request = Request(self.mq.pop())
            self.schedule(request)

    def stop(self):
        if self.thread and self.thread.is_alive():
            self._stop_event.set()
            self.thread.join()
    
    def instance_running(self, instance_id):
        return len(self.redis_client.hget(instance_id, "data_requests").decode('utf-8').split(',')) - 1 if self.redis_client.hexists(instance_id, "data_requests") else 0
    def schedule(self, request):
        value = self.redis_client.get(request.model)
        matched_ids = value.decode('utf-8').split(',')[1:] if value else []
        target = None
        max_running = 0
        slots = 0
        for iid in matched_ids:
            running = self.instance_running(iid)
            if running < BATCH_SIZE_LIMIT:
                if target is None or running > max_running:
                    max_running = running
                    target = iid
                slots += BATCH_SIZE_LIMIT - running
        if target is not None:
            print(f"schedule to {target} with {running} running requests")
            lua_script = """
            local current_value = redis.call('HGET', KEYS[1], ARGV[1])
            if current_value then
                current_value = current_value .. ARGV[2]
                redis.call('HSET', KEYS[1], ARGV[1], current_value)
            end
            return current_value
            """
            result = r.eval(lua_script, 1, target, "data_requests", ","+request.id)
            print(f"data_requests after append: {result}")
            mq = MessageQueue(iid)
            mq.push(request)
        else:
            raise Exception("No available instance")
        if slots <= BATCH_SIZE_LIMIT:
            self.swap(request.model)

    def swap(self, model):
        self.lock.acquire()
        # TODO swap
        self.lock.release()

class Instance:
    def __init__(self, id, models, cpu_model_num):
        self.id = id
        self.cpu_models = random.sample(models, cpu_model_num)
        self.gpu_model = self.cpu_models[0]
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.mq = MessageQueue(id, self.redis_client)
        self.lock = DistributedLock(LOCK_KEY, self.redis_client)
        self.start()

    def process_request(self, request):
        self.redis_client.hset(request.id, mapping={
            "status": "on",
            "timestamp": time.time(),
        })
        print("begin processing...", request.id)
        time.sleep(random.randint(0, request.input_length) / 10)
        print("done processing...", request.id)
        self.redis_client.hset(request.id, "status", "done")

    def process_swap(self, model):
        pass


    def loop(self):
        while not self._stop_event.is_set():
            request = Request(self.mq.pop())
            if request.is_swap:
                self.process_swap(request.model)
            else:
                self.process_request(request)
            time.sleep(0.1)

    def start(self):
        self.lock.acquire()
        self.redis_client.hset(self.id, mapping={
            "status": "ready",
            "last_request_time": 0,
            "swap_request": "None",
            "data_requests": "",
            "cpu_models": ",".join(self.cpu_models),
            "gpu_model": self.gpu_model,
        })
        self.redis_client.append(self.gpu_model, ','+self.id)
        self.lock.release()
        print(f"{self.id} with {self.gpu_model} out of {','.join(self.cpu_models)} is ready")
        self._stop_event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self._stop_event.set()
            self.thread.join()
        self.lock.acquire()
        # TODO redis remove instance from gpu_model list
        self.redis_client.delete(self.id)
        self.lock.release()
        

class Cluster:
    def __init__(self, models, n_instances=4, n_schedulers=4, n_api_servers=4, cpu_model_num=4):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        for m in models:
            self.redis_client.set(m, "")
        self.version = 0
        self.instances = [Instance(f'Instance-{self.version}-{i}', models, cpu_model_num) for i in range(n_instances)]
        self.schedulers = [Scheduler(f'Scheduler-{self.version}-{i}') for i in range(n_schedulers)]
        assert n_schedulers == n_api_servers
        self.api_servers = [ApiServer(f'Scheduler-{self.version}-{i}') for i in range(n_api_servers)]
        self.gateway = Gateway(self.api_servers)

    def instance_update(self, models, n_instances, cpu_model_num):
        self.version += 1
        new_instances = []
        assert n_instances >= len(self.instances) # simulate the case of instance scale up only
        for i in range(n_instances):
            if i < len(self.instances):
                self.instances[i].shutdown()
            new_instances.append(Instance(f'Instance-{self.version}-{i}', models, cpu_model_num))
        self.instances = new_instances




if __name__ == '__main__':
    models = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    cluster = Cluster()
    for i in range(10):
        r = random_request(models)
        cluster.gateway.on_request(r)
        time.sleep(1)
    time.sleep(100)
