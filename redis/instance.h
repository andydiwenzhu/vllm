#pragma once

#include "redis_client.h"
//#include "status.h"
#include <set>
#include <string>
#include <vector>


class Instance {
public:
    Instance();

    Instance(RedisClient& rc,
             const std::string& instanceId,
             const std::string& gpu_model,
             const std::vector<std::string>& cpu_models);

    int online();

    int offline();

    int on_request(const std::string& requestId);

    int done_request(const std::string& requestId);

    int on_swap(const std::string& requestId);

    int done_swap(const std::string& requestId, const std::string& gpu_model, const std::vector<std::string>& cpu_models);

private:
    std::string _cpu_models_to_string(const std::vector<std::string>& cpu_models) const;

private:
    RedisClient rc_;
    std::string instanceId_;
    std::string gpu_model_;
    std::string cpu_models_;
    std::set<std::string> running_data_requests_;
    std::string swap_request_id_;
};
