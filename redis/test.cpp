#include <cassert>
#include <chrono>
#include <hiredis/hiredis.h>
#include <thread>

#include "instance.h"
#include "scheduler.h"

void test_1_model_2_instances(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::string model = "Model-0";
    Instance i0(rc, "Instance-0", model, {model});
    i0.online();
    Instance i1(rc, "Instance-1", model, {model});
    i1.online();

    Scheduler s(rc);
    for (int i = 0; i < 10; ++i) {
        std::string request_id = "Request-" + std::to_string(i);
        auto result = s.schedule({request_id, model});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        if (result.first == "Instance-0") {
            i0.on_request(request_id);
        } else if (result.first == "Instance-1") {
            i1.on_request(request_id);
        }
        assert(result.first == (i < 5 ? "Instance-0" : "Instance-1"));
        assert(result.second == (i < 8 ? false : true));
    }
}

void test_data_swap_conflict(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::string model0 = "Model-0";
    std::string model1 = "Model-1";
    Instance i0(rc, "Instance-0", model0, {model0, model1});
    i0.online();
    Instance i1(rc, "Instance-1", model1, {model0, model1});
    i1.online();

    Scheduler s(rc);
    for (int i = 0; i < 3; ++i) {
        std::string request_id = "Request-" + std::to_string(i);
        auto result = s.schedule({request_id, model0});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        if (result.first == "Instance-0") {
            i0.on_request(request_id);
        } else if (result.first == "Instance-1") {
            i1.on_request(request_id);
        }
    }

    std::string rid3 = "Request-3";
    std::string rid4 = "Request-4";
    std::string rid5 = "Request-5";
    std::string sid = "Swap-Request-0";
    auto result = s.schedule({rid3, model0});

    assert(result.first == "Instance-0" && result.second);
    auto target = s.swap(model0, sid);
    assert(target == "Instance-1");
    i1.on_request(rid4);
    assert(i1.on_swap(sid) == 1);
    assert(i1.done_request(rid4) == 0);
    assert(i1.on_swap(sid) == 0);
    assert(i1.on_request(rid5) == 1);
    assert(i1.done_swap(sid, model0, {model0, model1}) == 0);

    for (int i = 0; i < 5; ++i) {
        std::string request_id = "Request-" + std::to_string(i + 10);
        auto result = s.schedule({request_id, model0});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        if (result.first == "Instance-0") {
            i0.on_request(request_id);
        } else if (result.first == "Instance-1") {
            i1.on_request(request_id);
        }
        assert(result.first == (i < 2 ? "Instance-0": "Instance-1"));
    }
}

void test_redis_client_empty_key(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::vector<std::string> results;
    rc.smembers("non-exists", results);
    assert(results.empty());
    rc.multi();
    rc.add("hmget", "non-exists", "key1", "key2", "key3", "key4");
    rc.add("smembers", "non-exists");
    rc.add("srem", "non-exists", "mem1", "mem2");
    rc.add("del", "non-exists");
    std::vector<std::vector<std::string>>results2;
    int r = rc.exec(&results2);
    assert(r == 0);
    for (int i = 0; i < results2[0].size(); ++i) {
        assert(results2[0][i] == "NIL");
    }
    for (int i = 1; i < results2.size(); ++i) {
        assert(results2[i].empty());
    }
}

void test_swap_race(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::vector<std::string> models;
    for (int i = 0; i < 3; ++i) {
        models.push_back("Model-" + std::to_string(i));
    }
    std::vector<Instance> instances;
    for (int i = 0; i < 3; ++i) {
        instances.emplace_back(rc, "Instance-" + std::to_string(i), models[i], models);
        instances[i].online();
    }
    Scheduler s(rc);
    for (int i = 0; i < 8; ++i) {
        std::string request_id = "Request-" + std::to_string(i);
        auto result = s.schedule({request_id, models[i%2]});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        if (result.first == "Instance-0") {
            instances[0].on_request(request_id);
        } else if (result.first == "Instance-1") {
            instances[1].on_request(request_id);
        }
        assert(result.second == (i >= 6));
    }
    auto target0 = s.swap(models[0], "Swap-Request-0");
    assert(target0 == "Instance-2");
    auto target1 = s.swap(models[1], "Swap-Request-1");
    assert(target1 == "");
    assert(instances[2].on_swap("Swap-Request-0") == 0);
    auto result = s.schedule({"Request-100", models[2]});
    assert(result.first == "" && result.second);
}

void test_swap_strategy(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::vector<std::string> models;
    for (int i = 0; i < 4; ++i) {
        models.push_back("Model-" + std::to_string(i));
    }
    Instance i0(rc, "Instance-0", models[0], {models[0], models[1]});
    i0.online();
    Instance i1(rc, "Instance-1", models[1], {models[1], models[2]});
    i1.online();
    Instance i2(rc, "Instance-2", models[2], {models[2], models[0]});
    i2.online();
    Instance i3(rc, "Instance-3", models[3], {models[3], models[0]});
    i3.online();

    Scheduler s(rc);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    s.schedule({"Request-100", models[3]});
    i3.on_request("Request-100");

    std::this_thread::sleep_for(std::chrono::seconds(1));
    s.schedule({"Request-101", models[2]});
    i2.on_request("Request-101");

    std::this_thread::sleep_for(std::chrono::seconds(1));
    i2.done_request("Request-101");

    std::this_thread::sleep_for(std::chrono::seconds(1));
    i3.done_request("Request-100");

    // i1 has the smallest last_request_time, but model-0 is not in cpu memory
    // for those with model-0 in cpu memory, i3 has a smaller last_request_time than i2
    // since we only consider the arrival time of request at this moment

    for (int i = 0; i < 4; ++i) {
        std::string request_id = "Request-" + std::to_string(i);
        auto result = s.schedule({request_id, models[0]});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        assert(result.first =="Instance-0");
        i0.on_request(request_id);
    }
    auto target = s.swap(models[0], "Swap-Request-0");
    assert(target == "Instance-3");
}

void test_swap_condition(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    std::string model0 = "Model-0";
    std::string model1 = "Model-1";
    Instance i0(rc, "Instance-0", model0, {model0, model1});
    i0.online();
    Instance i1(rc, "Instance-1", model1, {model0, model1});
    i1.online();
    Scheduler s(rc);
    for (int i = 0; i < 15; ++i) {
        std::string request_id = "Request-" + std::to_string(i);
        auto result = s.schedule({request_id, model0});
        std::cout << "schedule " << request_id << ": target=" << result.first << ", swap=" << result.second << std::endl;
        if (result.first == "Instance-0") {
            assert(i < 5);
            assert(i0.on_request(request_id) == 0);
        }
        if (result.second) {
            assert(i == 3);
            auto target = s.swap(model0, "Swap-Request-" + std::to_string(i));
            assert(target == "Instance-1");
        }
    }
}

void test_health_check(redisContext* redis_ctx, std::string cluster) {
    auto rc = RedisClient(redis_ctx, cluster+"-");
    assert(rc.multi() == 0);
    rc.add("hset", "cluster-info", "batch_size_limit", 2);
    rc.add("hset", "cluster-info", "reserve_batch_size", 1);
    rc.add("hset", "cluster-info", "data_request_timeout", 1); // very strict
    rc.add("hset", "cluster-info", "swap_request_timeout", 3); // very strict
    assert(rc.exec() == 0);

    std::string model0 = "Model-0";
    std::string model1 = "Model-1";
    Instance i0(rc, "Instance-0", model0, {model0, model1});
    i0.online();
    Instance i1(rc, "Instance-1", model1, {model0, model1});
    i1.online();
    Scheduler s(rc);

    auto result = s.schedule({"Request-0", model0});
    assert(result.first == "Instance-0" && !result.second);
    i0.on_request("Request-0");

    result = s.schedule({"Request-1", model0});
    assert(result.first == "Instance-0" && result.second);
    i0.on_request("Request-1");

    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto target = s.swap(model0, "Swap-Request-0");
    assert(target == "Instance-1");
    i1.on_swap("Swap-Request-0");

    result = s.schedule({"Request-2", model0}); // should trigger timeout warning for Instance-0
    assert(result.first == "" && !result.second);

    std::this_thread::sleep_for(std::chrono::seconds(4));
    result = s.schedule({"Request-3", model0}); // should trigger timeout warning for Instance-1
    assert(result.first == "" && !result.second);
}

std::string generateRandomString(int length) {
    std::srand(std::time(0));
    std::string possibleChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string randomString;
    for(int i=0; i<length; ++i) {
        randomString += possibleChars[std::rand() % possibleChars.size()];
    }
    return randomString;
}

void build_cluster(RedisClient& rc) {
    assert(rc.multi() == 0);
    rc.add("hset", "cluster-info", "batch_size_limit", 5);
    rc.add("hset", "cluster-info", "reserve_batch_size", 2);
    rc.add("hset", "cluster-info", "data_request_timeout", 30*60);
    rc.add("hset", "cluster-info", "swap_request_timeout", 60*60);
    assert(rc.exec() == 0);
}

void flushdb(redisContext* redis_ctx) {
    redisReply *reply = (redisReply*)redisCommand(redis_ctx, "FLUSHDB");
    freeReplyObject(reply);
}

int main() {
    auto redis_ctx = redisConnect("127.0.0.1", 6379);
    flushdb(redis_ctx);
    test_1_model_2_instances(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_data_swap_conflict(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_redis_client_empty_key(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_swap_race(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_swap_strategy(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_swap_condition(redis_ctx, generateRandomString(10));
    flushdb(redis_ctx);
    test_health_check(redis_ctx, generateRandomString(10));
    return 0;
}
