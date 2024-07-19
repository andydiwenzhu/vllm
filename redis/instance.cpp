#include "instance.h"

Instance::Instance() {}

Instance::Instance(RedisClient& rc,
            const std::string& instanceId,
            const std::string& gpu_model,
            const std::vector<std::string>& cpu_models) 
    : rc_(rc), instanceId_(instanceId), gpu_model_(gpu_model), cpu_models_(_cpu_models_to_string(cpu_models)) {}
int Instance::online() {
    rc_.lock();
    if (rc_.multi()) return 1;
    rc_.add("sadd", "cluster-instances", instanceId_);
    rc_.add("sadd", gpu_model_, instanceId_);
    rc_.add("hset", instanceId_, "gpu_model", gpu_model_);
    rc_.add("hset", instanceId_, "cpu_models", cpu_models_);
    rc_.add("hset", instanceId_, "status", "ready");
    rc_.add("hset", instanceId_, "last_request_time", std::to_string(time(NULL)));
    std::cout << instanceId_ << " " << gpu_model_ << " " << cpu_models_ << " " << std::to_string(time(NULL)) << std::endl;
    int r = rc_.exec();
    rc_.unlock();
    return r;
}

int Instance::offline() {
    rc_.lock();
    if (rc_.multi()) return 1;
    rc_.add("srem", "cluster-instances", instanceId_);
    rc_.add("srem", gpu_model_, instanceId_);
    rc_.add("del", instanceId_);
    int r = rc_.exec();
    rc_.unlock();
    return r;
}

int Instance::on_request(const std::string& requestId) {
    if (swap_request_id_ != "") {
        std::cerr << "Instance is still in swap, failed request: " << requestId << std::endl;
        return 1;
    } else {
        running_data_requests_.insert(requestId);
    }
    if (rc_.multi()) return 1;
    rc_.add("hset", requestId, "status", "on");
    rc_.add("hset", requestId, "timestamp", std::to_string(time(NULL)));
    return rc_.exec();
}

int Instance::done_request(const std::string& requestId) {
    running_data_requests_.erase(requestId);
    if (rc_.multi()) return 1;
    rc_.add("hset", requestId, "status", "done");
    return rc_.exec();
}

int Instance::on_swap(const std::string& requestId) {
    if (running_data_requests_.size() > 0) {
        std::cerr << "Some request is still running, failed request: " << requestId << std::endl;
        return 1;
    } else if (swap_request_id_ != "") {
        std::cerr << "Instance is still in swap, failed request: " << requestId << std::endl;
        return 1;
    } else {
        swap_request_id_ = requestId;
    }
    if (rc_.multi()) return 1;
    rc_.add("hset", requestId, "status", "on");
    rc_.add("hset", requestId, "timestamp", std::to_string(time(NULL)));
    return rc_.exec();
}

int Instance::done_swap(const std::string& requestId, const std::string& gpu_model, const std::vector<std::string>& cpu_models) {
    swap_request_id_ = "";
    gpu_model_ = gpu_model;
    cpu_models_ = _cpu_models_to_string(cpu_models);
    if (rc_.multi()) return 1;
    rc_.add("hset", requestId, "status", "done");
    rc_.add("hset", requestId, "gpu_model", gpu_model_);
    rc_.add("hset", requestId, "cpu_models", cpu_models_);
    return rc_.exec();     
}

std::string Instance::_cpu_models_to_string(const std::vector<std::string>& cpu_models) const {
    std::string s = "";
    if (cpu_models.size() > 0) {
        s += cpu_models[0];
        for (int i = 1; i < cpu_models.size(); i++) {
            s += "," + cpu_models[i];
        }
    }
    return s;
}
