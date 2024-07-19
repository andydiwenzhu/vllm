#pragma once

#include <hiredis/hiredis.h>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

class RedisClient {
public:
    RedisClient();
    RedisClient(RedisClient&& other) noexcept;
    RedisClient(RedisClient& rc);
    RedisClient(redisContext* context, std::string prefix);

    bool lock(int timeoutMs = 60000);

    bool unlock();

    int multi();

    template<typename... Args>
    void add(const std::string& command, const std::string& key, Args... args) {
        std::ostringstream oss;
        oss << "local r_" << command_num_ << " = redis.call('" << command << "', '" << prefix_+key;
        ((oss << "', '" << args), ...); // 使用折叠表达式展开参数
        oss << "')\n";
        lua_script_ += oss.str();
        command_num_++;
    }

    void add(const std::string& command, const std::string& key, const std::vector<std::string>& args);

    int exec(std::optional<std::vector<std::vector<std::string>>*> results = std::nullopt);

    int smembers(const std::string& key, std::vector<std::string>& members);

    int sadd(const std::string& key, const std::string& member);

    template<typename... Args>
    int hmget(std::vector<std::string>& results, const std::string& key, Args... args) {
            std::ostringstream oss;
        oss << "hmget " << prefix_+key;
        ((oss << " " << args), ...); // 使用折叠表达式展开参数
        
        redisReply* reply = (redisReply*)redisCommand(context_, oss.str().c_str());
        if (reply == NULL) {
            std::cerr << "Redis command failed: " << context_->errstr << std::endl;
            return 1;
        } else if (reply->type != REDIS_REPLY_ARRAY) {            
            printf("Unexpected reply type for SMEMBERS: %d\n", reply->type);
            freeReplyObject(reply); 
            return 1;
        } else {
            results.resize(reply->elements);
            for (size_t i = 0; i < reply->elements; ++i) {
                if (reply->element[i]->type == REDIS_REPLY_NIL) {
                    results[i] = "NIL";
                } else if (reply->element[i]->type == REDIS_REPLY_STRING) {
                    results[i] = reply->element[i]->str;
                } else {
                    return 1;
                }
            }
            freeReplyObject(reply); 
        }

        return 0;
    }

private:
    redisContext* context_;
    std::string prefix_;
    std::string lua_script_;
    int command_num_ = 0;
    bool locked_ = false;
    bool in_multi_ = false;
};
