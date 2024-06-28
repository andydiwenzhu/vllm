import redis
import time
from uuid import uuid4

class DistributedLock:
    def __init__(self, lock_key, redis_client=redis.Redis(host='localhost', port=6379, db=0)):
        self.redis_client = redis_client
        self.lock_key = lock_key
        self.lock_value = None

    def acquire(self, timeout=10000, sleep_interval=0.1):
        """
        尝试获取锁，如果在指定时间内未获取到则返回False
        :param timeout: 超时时间，单位毫秒
        :param sleep_interval: 获取锁失败后，休眠的时间间隔
        :return: 是否成功获取锁
        """
        end = time.time() + timeout / 1000.0
        while time.time() < end:
            self.lock_value = str(uuid4())
            if self.redis_client.setnx(self.lock_key, self.lock_value):
                # 设置锁的超时时间，防止进程崩溃导致锁无法释放
                self.redis_client.pexpire(self.lock_key, timeout)
                return True
            time.sleep(sleep_interval)
        return False

    def release(self):
        """
        释放锁
        :return: 是否成功释放锁
        """
        # 使用lua脚本保证操作的原子性，避免因客户端崩溃导致的锁误释放
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        return self.redis_client.eval(lua_script, 1, self.lock_key, self.lock_value) == 1

# 使用示例
if __name__ == "__main__":
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    lock = DistributedLock(redis_client, 'my_lock')

    if lock.acquire():
        print("Lock acquired.")
        try:
            # 在这里执行需要锁保护的代码
            pass
        finally:
            # 确保锁总是被释放
            if lock.release():
                print("Lock released.")
            else:
                print("Failed to release lock.")
    else:
        print("Failed to acquire lock.")
