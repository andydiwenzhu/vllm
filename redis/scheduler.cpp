#include "scheduler.h"
#include <algorithm>

Scheduler::Scheduler(RedisClient& rc) : rc_(rc), status_(rc_) {}

std::pair<std::string, bool> Scheduler::schedule(Request r) {
    std::string target = "";
    auto iids = status_.get_instances_for_model(r.model);
    if (iids.size() == 0) {
        return std::pair<std::string, bool>(target, true);
    }

    status_.sync_instances(iids);
    status_.update_instances(iids);
    int batch_size_limit = status_.get_batch_size_limit();
    int slots = 0;
    int target_running = -1;
    for (auto& iid: iids) {
        if (status_.get_instance_status(iid).status == "swap") {
            slots += batch_size_limit;
        } else {
            int running = status_.get_instance_status(iid).running;
            
            if (running < batch_size_limit) {
                if (running > target_running) {
                    target_running = running;
                    target = iid;
                }
                slots += batch_size_limit - running;
            }
        }
    }
    //std::cout << "target: " << target << ", target_running: " << target_running << ", slots:" << slots << std::endl;
    if (target_running < 0) {
        return std::pair<std::string, bool>(target, slots <= status_.get_reserve_batch_size());
    }

    status_.add_data_request(target, r.requestId);

    return std::pair<std::string, bool>(target, slots <= status_.get_reserve_batch_size());
}

std::string Scheduler::swap(std::string model, std::string swap_request_id) {
    rc_.lock();

    auto iids = status_.get_all_instances();
    status_.sync_instances(iids);

    // begin batching updates and commit them all at once in the end
    status_.start_update_batch();
    status_.update_instances(iids);

    std::vector<std::pair<int, std::string>> in_mems;
    std::vector<std::pair<int, std::string>> out_mems;

    for (auto& iid: iids) {
        auto istatus = status_.get_instance_status(iid);
        if (istatus.status == "ready" && istatus.gpu_model != model && istatus.running == 0) {
            if (istatus.cpu_models.find(model) != std::string::npos) {
                in_mems.push_back(std::pair<int, std::string>(istatus.last_request_time, iid));
            } else {
                out_mems.push_back(std::pair<int, std::string>(istatus.last_request_time, iid));
            }
        }
    }

    std::string target = "";
    if (in_mems.size() > 0) {
        std::sort(in_mems.begin(), in_mems.end());
        target = in_mems[0].second;
    } else if (out_mems.size() > 0) {
        std::sort(out_mems.begin(), out_mems.end());
        target = out_mems[0].second;
    }

    if (!target.empty()) {
        status_.add_swap_request(target, model, swap_request_id);
    }

    status_.commit_update_batch();
    rc_.unlock();
    return target;
}
