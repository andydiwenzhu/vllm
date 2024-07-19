
#include "status.h"

Status::Status() {}
Status::Status(RedisClient& rc) : rc_(rc) {
    sync_cluster_info();
}

int Status::sync_cluster_info() {
    std::vector<std::string> results;
    int r = rc_.hmget(results, "cluster-info", "batch_size_limit", "reserve_batch_size", "data_request_timeout", "swap_request_timeout");
    if (r == 0 && results.size() == 4) {
        batch_size_limit_ = results[0] == "NIL" ? batch_size_limit_ : std::stoi(results[0]);
        reserve_batch_size_ = results[1] == "NIL" ? reserve_batch_size_ : std::stoi(results[1]);
        data_request_timeout_ = results[2] == "NIL" ? data_request_timeout_ : std::stoi(results[2]);
        swap_request_timeout_ = results[3] == "NIL" ? swap_request_timeout_ : std::stoi(results[3]);
        //std::cout << batch_size_limit_ << " " << reserve_batch_size_ << " " << data_request_timeout_ << " " << swap_request_timeout_ << std::endl;
    } else {
        std::cerr << "Failed to get cluster-info from redis" << std::endl;
    }
    return r;
}

int Status::get_batch_size_limit() {
    return batch_size_limit_;
}

int Status::get_reserve_batch_size() {
    return reserve_batch_size_;
}

int Status::sync_instances_for_model(const std::string& model) {
    std::vector<std::string> iids;
    if (rc_.smembers(model, iids) == 0) {
        model2instances_[model] = iids;
    } else {
        std::cerr << "Failed to get instance ids for model " << model << std::endl;
        return 1;
    }
    return 0;
}

std::vector<std::string> Status::get_instances_for_model(const std::string& model) {
    if (sync_instances_for_model(model) != 0 && model2instances_.find(model) == model2instances_.end()) {
        return {};
    }
    return model2instances_[model];
}

int Status::sync_live_instances() {
    std::vector<std::string> iids;
    if (rc_.smembers("cluster-instances", iids) == 0) {
        live_instances_ = iids;
    } else {
        std::cerr << "Failed to get live instances from redis" << std::endl;
        return 1;
    }
    return 0;
}

std::vector<std::string> Status::get_all_instances() {
    sync_live_instances();
    return live_instances_;
}

const InstanceStatus& Status::get_instance_status(const std::string& iid) {
    return instance_status_[iid];
}

int Status::sync_i2r(const std::vector<std::string>& iids) {
    if (rc_.multi()) return 1;
    for (auto& iid: iids) {
        rc_.add("smembers", "I2R-"+iid);
    }
    std::vector<std::vector<std::string>> results;
    int r = rc_.exec(&results);
    std::vector<std::string> rids;
    if (r == 0 && results.size() == iids.size()) {
        for (int i = 0; i < iids.size(); ++i) {
            i2r_[iids[i]] = results[i];
            rids.insert(rids.end(), results[i].begin(), results[i].end());
        }
    }

    results.clear();
    if (rc_.multi()) return 1;
    for (auto& rid: rids) {
        rc_.add("hmget", rid, "status", "timestamp");
    }
    r = rc_.exec(&results);
    if (r == 0 && results.size() == rids.size()) {
        for (int i = 0; i < rids.size(); ++i) {
            data_request_status_[rids[i]] = {results[i][0], results[i][1] == "NIL" ? 0 : std::stoi(results[i][1])};
        }
    }
    return 0;
}

void Status::update_i2r(const std::vector<std::string>& iids) {
    for (auto& iid: iids) {
        std::vector<std::string> done_data_requests;
        int running = 0;
        int last_request_time = instance_status_[iid].last_request_time;
        int cnt = 0;
        for (auto& rid: i2r_[iid]) {
            if (data_request_status_.find(rid) != data_request_status_.end()) {
                if (data_request_status_[rid].status == "on") {
                    running++;
                    if (time(NULL) - data_request_status_[rid].timestamp > data_request_timeout_) {
                        cnt++;
                        // we only give warnings at this time
                        // rc_.add("srem", "cluster-instances", iid);
                        // rc_.add("srem", instance_status_[iid].gpu_model, iid);
                    }
                } else {
                    done_data_requests.push_back(rid);
                    if (data_request_status_[rid].timestamp > last_request_time) {
                        last_request_time = data_request_status_[rid].timestamp;
                    }
                }
            }
        }
        if (cnt > 0) {
            std::cerr << iid << " might be in trouble, as it has " << cnt << 
                " data request running for more than " << data_request_timeout_ << " seconds" << std::endl;
        }
        instance_status_[iid].running = running;

        if (last_request_time > instance_status_[iid].last_request_time) {
            instance_status_[iid].last_request_time = last_request_time;
            rc_.add("hset", iid, "last_request_time", last_request_time);
        }

        if (done_data_requests.size() > 0) {
            rc_.add("srem", "I2R-"+iid, done_data_requests);
        }
    }
}

int Status::sync_i2s(const std::vector<std::string>& iids) {
    if (rc_.multi()) return 1;
    for (auto& iid: iids) {
        rc_.add("hmget", iid, "swap_request_id");
    }
    std::vector<std::vector<std::string>> results;
    int r = rc_.exec(&results);
    std::vector<std::string> rids;
    if (r == 0 && results.size() == iids.size()) {
        for (int i = 0; i < iids.size(); ++i) {
            if (results[i].size() > 0) {
                instance_status_[iids[i]].swap_request_id = results[i][0];
                rids.push_back(results[i][0]);
            }
        }
    }

    results.clear();
    if (rc_.multi()) return 1;
    for (auto& rid: rids) {
        rc_.add("hmget", rid, "status", "timestamp", "gpu_model", "cpu_models");
    }
    r = rc_.exec(&results);
    if (r == 0 && results.size() == rids.size()) {
        for (int i = 0; i < rids.size(); ++i) {
            swap_request_status_[rids[i]] = {results[i][0], 
                results[i][1] == "NIL" ? 0 : std::stoi(results[i][1]), 
                results[i][2], results[i][3]};
        }
    }

    return 0;
}

void Status::update_i2s(const std::vector<std::string>& iids) {
    for (auto& iid: iids) {
        if (instance_status_[iid].status != "swap") continue;
        auto swap_request_id = instance_status_[iid].swap_request_id;
        if (swap_request_status_.find(swap_request_id) != swap_request_status_.end()) {
            if (swap_request_status_[swap_request_id].status == "done") {
                instance_status_[iid].status = "ready";
                instance_status_[iid].gpu_model = swap_request_status_[swap_request_id].gpu_model;
                instance_status_[iid].cpu_models = swap_request_status_[swap_request_id].cpu_models;
                instance_status_[iid].swap_request_id = "";

                rc_.add("hset", iid, "status", "ready");
                rc_.add("hset", iid, "gpu_model", swap_request_status_[swap_request_id].gpu_model);
                rc_.add("hset", iid, "cpu_models", swap_request_status_[swap_request_id].cpu_models);
                rc_.add("hset", iid, "swap_request_id", "");
            }
            if (swap_request_status_[swap_request_id].status == "failed") {
                rc_.add("srem", swap_request_status_[swap_request_id].gpu_model, iid);
                rc_.add("sadd", instance_status_[iid].gpu_model, iid);

                instance_status_[iid].status = "ready";
                instance_status_[iid].swap_request_id = "";

                rc_.add("hset", iid, "status", "ready");
                rc_.add("hset", iid, "swap_request_id", "");
            }
            if (swap_request_status_[swap_request_id].status == "on") {
                if (time(NULL) - swap_request_status_[swap_request_id].timestamp > swap_request_timeout_) {
                    std::cerr << iid << " might be in trouble, as it has a swap " <<
                        "request running for more than " << swap_request_timeout_ << " seconds" << std::endl;
                }
            }
            if (swap_request_status_[swap_request_id].timestamp > instance_status_[iid].last_request_time) {
                instance_status_[iid].last_request_time = swap_request_status_[swap_request_id].timestamp;
                rc_.add("hset", iid, "last_request_time", instance_status_[iid].last_request_time);
            }
        }
    }
}

int Status::sync_instances(const std::vector<std::string>& iids) {
    if (rc_.multi()) return 1;
    for (auto& iid : iids) {
        rc_.add("hmget", iid, "status", "gpu_model", "cpu_models", "last_request_time");
    }
    std::vector<std::vector<std::string>> results;
    int r = rc_.exec(&results);
    if (r == 0 && results.size() == iids.size() && results[0].size() == 4) { 
        std::vector<std::string> swap_iids;
        for (int i = 0; i < iids.size(); ++i) {
            if (instance_status_.find(iids[i]) == instance_status_.end()) {
                instance_status_[iids[i]] = InstanceStatus();
            }
            instance_status_[iids[i]].status = results[i][0];
            instance_status_[iids[i]].gpu_model = results[i][1];
            instance_status_[iids[i]].cpu_models = results[i][2];
            instance_status_[iids[i]].last_request_time = std::stoi(results[i][3]);
            if (results[i][0] == "swap") {
                swap_iids.push_back(iids[i]);
            }
        }
        if (sync_i2r(iids)) return 1;
        if (sync_i2s(swap_iids)) return 1;
    } else {
        return 1;
    }
    return 0;
}

int Status::start_update_batch() {
    return rc_.multi();
}

void Status::update_instances(const std::vector<std::string>& iids) {
    update_i2r(iids);
    update_i2s(iids);
}
void Status::add_data_request(const std::string& iid, const std::string& rid) {
    rc_.sadd("I2R-"+iid, rid);
    i2r_[iid].push_back(rid);
}

void Status::add_swap_request(const std::string& iid, const std::string& model, const std::string& sid) {
    rc_.add("hset", iid, "status", "swap");
    rc_.add("hset", iid, "swap_request_id", sid);
    rc_.add("sadd", model, iid);
    rc_.add("srem", instance_status_[iid].gpu_model, iid);
    instance_status_[iid].status = "swap";
    instance_status_[iid].swap_request_id = sid;
    model2instances_[model].push_back(iid);
    // remove iid from model2instances_[instance_status_[iid].gpu_model]
    for (int i = 0; i < model2instances_[instance_status_[iid].gpu_model].size(); ++i) {
        if (model2instances_[instance_status_[iid].gpu_model][i] == iid) {
            model2instances_[instance_status_[iid].gpu_model].erase(model2instances_[instance_status_[iid].gpu_model].begin() + i);
        }
    }
}

int Status::commit_update_batch() {
    return rc_.exec();
}
