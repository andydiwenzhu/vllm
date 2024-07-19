import logging
import random
import redis
import sys
import threading
import time

from lock import DistributedLock
from message_queue import MessageQueue
from request import Request, random_request

LOCK_KEY = 'global-instance-lock'
BATCH_SIZE_LIMIT = 5    
RESERVE_SLOTS = 2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(module)s.%(funcName)s|%(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

remove_lua_script = """
local key = KEYS[1]
local valueToRemove = ARGV[1]

local currentValue = redis.call('get', key)
if currentValue then
    local values = {}
    for value in string.gmatch(currentValue, '([^,]+)') do
        if value ~= valueToRemove then
            table.insert(values, value)
        end
    end
    local newValue = table.concat(values, ',')
    redis.call('set', key, newValue)
end
"""

remove_done_lua_script = """
local function removeCompletedRequests(sourceKey)
    local requests = redis.call('hget', sourceKey, 'data_requests')
    if not requests then
        return 0 -- 没有找到data_requests字段
    end

    local ids = {}
    for id in string.gmatch(requests, '([^,]+)') do
        table.insert(ids, id)
    end

    local updatedRequests = {}
    for _, id in ipairs(ids) do
        local status = redis.call('hget', id, 'status')
        if status ~= 'done' then
            table.insert(updatedRequests, id)
        end
    end

    local newRequestsStr = table.concat(updatedRequests, ',')
    redis.call('hset', sourceKey, 'data_requests', newRequestsStr)

    return #ids - #updatedRequests -- 返回移除的已完成请求的数量
end

return removeCompletedRequests(KEYS[1])
"""


class Gateway:
    def __init__(self, api_servers):
        self.api_servers = api_servers
        self.cnt = 0
    def on_request(self, request):
        logger.info(f"{request.id}|OnGateway")
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
            request = self.mq.pop()
            if request is None:
                time.sleep(0.1)
                continue
            self.schedule(request)

    def stop(self):
        if self.thread and self.thread.is_alive():
            self._stop_event.set()
            self.thread.join()
    
    def instance_running(self, instance_id):
        r = self.redis_client.hget(instance_id, "data_requests")
        logger.debug(f"instance_running: {instance_id} {r.decode('utf-8')}")
        if r is None:
            return 0
        return len(r.decode('utf-8').split(',')) - 1
    def schedule(self, request):
        logger.info(f"{request.id}|OnSchedule: model={request.model}")
        value = self.redis_client.get(request.model)
        matched_ids = value.decode('utf-8').split(',')[1:] if value else []
        logger.debug(f"matched: {matched_ids}")
        target = None
        max_running = 0
        slots = 0
        for iid in matched_ids:
            if self.redis_client.hget(iid, "status").decode('utf-8') == "swap":
                srid = self.redis_client.hget(iid, "swap_request_id").decode('utf-8')
                if srid != "None" and self.redis_client.hget(srid, "gpu_model").decode('utf-8') == request.model:
                    slots -= BATCH_SIZE_LIMIT
                continue
            running = self.instance_running(iid)
            if running < BATCH_SIZE_LIMIT:
                if target is None or running > max_running:
                    max_running = running
                    target = iid
                slots += BATCH_SIZE_LIMIT - running
        if target is not None:
            logger.debug(f"schedule to {target} with {running} running requests")
            lua_script = """
            local current_value = redis.call('HGET', KEYS[1], ARGV[1])
            if current_value then
                current_value = current_value .. ARGV[2]
                redis.call('HSET', KEYS[1], ARGV[1], current_value)
            end
            return current_value
            """
            result = self.redis_client.eval(lua_script, 1, target, "data_requests", ","+request.id)
            logger.debug(f"data_requests after append: {result}")
            mq = MessageQueue(target)
            mq.push(request)
        else:
            logger.info(f"{request.id}|Failover to CPU")
        if slots <= RESERVE_SLOTS:
            self.swap(request.model)

    def lazy_update(self):
        instances = self.redis_client.get("INS_LIST").decode('utf-8').split(',')[1:]
        logger.debug(f"lazy update: {instances}")
        for iid in instances:
            status = self.redis_client.hget(iid, "status")
            if status is None:
                raise ValueError(f"Instance {iid} has no status")
            status = status.decode('utf-8')
            if status == "swap":
                srid = self.redis_client.hget(iid, "swap_request_id")
                if srid is None:
                    raise ValueError(f"Instance {iid} has no swap request id but its status is swap")
                rstatus = self.redis_client.hget(srid, "status")
                if rstatus is not None and rstatus.decode('utf-8') == "done":
                    last_request_time = self.redis_client.hget(srid, "timestamp").decode('utf-8')
                    new_cpu_models = self.redis_client.hget(srid, "cpu_models").decode('utf-8')
                    new_gpu_model = self.redis_client.hget(srid, "gpu_model").decode('utf-8')
                    gpu_model = self.redis_client.hget(iid, "gpu_model").decode('utf-8')
                    logger.debug(f"Reset to Ready: cpu: {new_cpu_models}, new: {new_gpu_model}, old: {gpu_model}")
                    self.redis_client.eval(remove_lua_script, 1, gpu_model, iid)
                    self.redis_client.append(new_gpu_model, ','+iid)
                    self.redis_client.hset(iid, mapping={
                        "gpu_model": new_gpu_model,
                        "cpu_models": new_cpu_models,
                        "swap_request": "None",
                        "data_requests": "None",
                        "last_request_time": last_request_time,
                        "status": "ready"
                    })
            else:
                last_request_time = float(self.redis_client.hget(iid, "last_request_time").decode("utf-8"))
                data_requests = self.redis_client.hget(iid, "data_requests").decode("utf-8")
                for rid in data_requests.split(",")[1:]:
                    rstatus = self.redis_client.hget(rid, "status")
                    if rstatus is not None:
                        last_request_time = max(float(self.redis_client.hget(rid, "timestamp").decode('utf-8')), last_request_time)
                self.redis_client.hset(iid, "last_request_time", last_request_time)
                removed = self.redis_client.eval(remove_done_lua_script, 1, iid)
                logger.debug(f"{iid} removed {removed} done requests")


    def swap(self, model):
        self.lock.acquire()

        self.lazy_update()

        logger.debug(f"swap for model {model}")
        instances = self.redis_client.get("INS_LIST").decode('utf-8').split(',')[1:]
        logger.debug(f"all instances: {instances}")  
        in_mem = (None, 1e30)
        out_mem = (None, 1e30)

        for iid in instances:
            status, gpu_model, cpu_models, last_request_time = self.redis_client.hmget(iid, "status", "gpu_model", "cpu_models", "last_request_time")
            status, gpu_model, cpu_models, last_request_time = status.decode('utf-8'), gpu_model.decode('utf-8'), cpu_models.decode('utf-8'), float(last_request_time.decode('utf-8'))
            running = self.instance_running(iid)
            logger.debug("iid: {}, status: {}, gpu_model: {}, cpu_models: {}, last_request_time: {}, running: {}".format(iid, status, gpu_model, cpu_models, last_request_time, running))
            if status == "ready" and gpu_model != model and running == 0:
                if model in cpu_models:
                    if last_request_time < in_mem[1]:
                        in_mem = (iid, last_request_time)
                else:
                    if last_request_time < out_mem[1]:
                        out_mem = (iid, last_request_time)

        if in_mem[0] is not None:
            target = in_mem[0]  
        elif out_mem[0] is not None:
            target = out_mem[0]
        else:
            logger.error(f"No Instance Available to Swap for {model}")
            self.lock.release()
            return
        
        swap_request = Request(model=model, input_length=-1, is_swap=True)
        self.redis_client.hset(target, mapping={
            "status": "swap",
            "swap_request_id": swap_request.id
        })
        mq = MessageQueue(target)
        mq.push(swap_request)
        logger.debug(f"swap request sent for model {model} to {target}")

        self.lock.release()

class Instance:
    def __init__(self, id, models, cpu_model_num, gpu_model=None):
        self.id = id
        self.cpu_models = random.sample(models, cpu_model_num)
        self.gpu_model = self.cpu_models[0] if gpu_model is None else gpu_model
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.mq = MessageQueue(id, self.redis_client)
        self.lock = DistributedLock(LOCK_KEY, self.redis_client)
        self.threads = []
        self.start()

    def process_request(self, request):
        self.redis_client.hset(request.id, mapping={
            "status": "on",
            "timestamp": time.time(),
        })
        logger.info(f"{request.id}|begin processing request of model {request.model} on {self.id}...")
        time.sleep(random.randint(0, request.input_length) / 10)
        logger.info(f"{request.id}|done processing request of model {request.model} on {self.id}...")
        self.redis_client.hset(request.id, "status", "done")

    def process_swap(self, request):
        logger.debug(f"{self.id} begin swapping from model {self.gpu_model} to {request.model} ...")
        self.redis_client.hset(request.id, mapping={
            "status": "on",
            "gpu_model": request.model,
            "timestamp": time.time(),
        })
        if request.model not in self.cpu_models:
            self.cpu_models = self.cpu_models[1:]
            self.cpu_models.append(request.model)
            time.sleep(10)
        else:
            time.sleep(3)
        
        self.gpu_model = request.model
        self.redis_client.hset(request.id, mapping={
            "status": "done",
            "cpu_models": ",".join(self.cpu_models),
        })
        logger.debug(f"{self.id} done swapping to gpu: {self.gpu_model}, cpu: {self.cpu_models}")

    def loop(self):
        while not self._stop_event.is_set():
            request = self.mq.pop()
            if request is None:
                lives = []
                for t in self.threads:
                    if t.is_alive():
                        lives.append(t)
                    else:
                        t.join()
                self.threads = lives
                time.sleep(0.1)
                continue
            if request.is_swap:
                self.process_swap(request)
            else:
                t = threading.Thread(target=self.process_request, args=(request,))
                t.start()
                self.threads.append(t)
                
    def start(self):
        self.lock.acquire()
        self.redis_client.hset(self.id, mapping={
            "status": "ready",
            "last_request_time": 0,
            "swap_request": "None",
            "data_requests": "None",
            "cpu_models": ",".join(self.cpu_models),
            "gpu_model": self.gpu_model,
        })
        self.redis_client.append(self.gpu_model, ','+self.id)
        self.redis_client.append("INS_LIST", ","+self.id)
        self.lock.release()
        logger.debug(f"{self.id} with {self.gpu_model} out of {','.join(self.cpu_models)} is ready")
        self._stop_event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self._stop_event.set()
            self.thread.join()
        self.lock.acquire()
        self.redis_client.eval(remove_lua_script, 1, self.gpu_model, self.id)
        self.redis_client.eval(remove_lua_script, 1, "INS_LIST", self.id)
        self.redis_client.delete(self.id)
        self.lock.release()
        

class Cluster:
    def __init__(self, models, n_instances=4, n_schedulers=4, n_api_servers=4, cpu_model_num=4, gpu_models=None):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.redis_client.set("INS_LIST", "None")
        for m in models:
            self.redis_client.set(m, "None")
        self.version = 0
        if gpu_models is None:
            self.instances = [Instance(f'Instance-{self.version}-{i}', models, cpu_model_num) for i in range(n_instances)]
        else:
            self.instances = [Instance(f'Instance-{self.version}-{i}', models, cpu_model_num, gpu_models[i]) for i in range(n_instances)]
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
    # models = ['A', 'B', 'C', 'D']#, 'E', 'F', 'G', 'H']
    # cluster = Cluster(models, gpu_models=['A', 'A', 'A', 'B'])
    # time.sleep(1)
    # cluster.gateway.on_request(Request('C'))
    # time.sleep(1)
    # cluster.gateway.on_request(Request('D'))
    # time.sleep(1)
    # for i in range(10):
    #     cluster.gateway.on_request(Request('A'))
    #     time.sleep(1)
    #     cluster.gateway.on_request(Request('B'))
    #     time.sleep(1)
    # exit()
    models = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    cluster = Cluster(models, n_instances=12, cpu_model_num=4)
    time.sleep(1)
    for i in range(600):
        x = (i // 100) % 6
        r = random_request(models[x: x+3])
        cluster.gateway.on_request(r)
        if i % 10 == 9:
            time.sleep(5)
        else:
            time.sleep(0.5)
    time.sleep(60)
