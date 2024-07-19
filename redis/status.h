#pragma once

#include "redis_client.h"
#include <iostream>
#include <map>
#include <string>
#include <vector>


struct Request {
    std::string requestId;
    std::string model;
};

struct InstanceStatus {
    // i2s related
    std::string status;
    std::string gpu_model;
    std::string cpu_models;
    std::string swap_request_id;
    // i2r related
    int running = 0;
    // common
    int last_request_time = 0;
};

struct DataRequestStatus {
    std::string status;
    int timestamp;
};

struct SwapRequestStatus {
    std::string status;
    int timestamp;
    std::string gpu_model;
    std::string cpu_models;
};


class Status {
public:
    Status();

    Status(RedisClient& rc);

    int sync_cluster_info();

    int get_batch_size_limit();

    int get_reserve_batch_size();

    int sync_instances_for_model(const std::string& model);

    std::vector<std::string> get_instances_for_model(const std::string& model);

    int sync_live_instances();

    std::vector<std::string> get_all_instances();

    const InstanceStatus& get_instance_status(const std::string& iid);

    int sync_i2r(const std::vector<std::string>& iids);

    void update_i2r(const std::vector<std::string>& iids);

    int sync_i2s(const std::vector<std::string>& iids);

    void update_i2s(const std::vector<std::string>& iids);

    int sync_instances(const std::vector<std::string>& iids);

    int start_update_batch();

    void update_instances(const std::vector<std::string>& iids);

    void add_data_request(const std::string& iid, const std::string& rid);

    void add_swap_request(const std::string& iid, const std::string& model, const std::string& sid);

    int commit_update_batch();

private:
    RedisClient rc_;
    //std::vector<std::string> models_;
    std::vector<std::string> live_instances_;
    std::map<std::string, std::vector<std::string>> model2instances_;
    std::map<std::string, InstanceStatus> instance_status_;
    std::map<std::string, std::vector<std::string>> i2r_;
    std::map<std::string, DataRequestStatus> data_request_status_;
    std::map<std::string, SwapRequestStatus> swap_request_status_;

    int batch_size_limit_ = 5;
    int reserve_batch_size_ = 2;
    int data_request_timeout_ = 60 * 60;
    int swap_request_timeout_ = 60 * 60;
};
