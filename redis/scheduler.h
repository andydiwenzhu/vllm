#pragma once

#include "redis_client.h"
#include "status.h"

class Scheduler {
public:
    Scheduler(RedisClient& rc);

    std::pair<std::string, bool> schedule(Request r);

    std::string swap(std::string model, std::string swap_request_id);

private:
    RedisClient rc_;
    Status status_;
};
