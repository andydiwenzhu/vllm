#include "redis_client.h"

RedisClient::RedisClient() {}
RedisClient::RedisClient(RedisClient&& other) noexcept {
    context_ = std::move(other.context_);
    prefix_ = std::move(other.prefix_);
}
RedisClient::RedisClient(RedisClient& rc) : context_(rc.context_), prefix_(rc.prefix_) {}
RedisClient::RedisClient(redisContext* context, std::string prefix) : context_(context), prefix_(prefix) {}

bool RedisClient::lock(int timeoutMs) {
    std::string script = "if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end";
    redisReply* reply = (redisReply*)redisCommand(context_, "EVAL %s 1 %s %s %d", script.c_str(), prefix_.c_str(), prefix_.c_str(), timeoutMs);
    if (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        freeReplyObject(reply);
        locked_ = true;
        return true;
    }
    freeReplyObject(reply);
    return false;
}

bool RedisClient::unlock() {
    if (!locked_) return false;
    std::string script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    redisReply* reply = (redisReply*)redisCommand(context_, "EVAL %s 1 %s %s", script.c_str(), prefix_.c_str(), prefix_.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        freeReplyObject(reply);
        locked_ = false;
        return true;
    }
    freeReplyObject(reply);
    return false;
}

int RedisClient::multi() {
    if (in_multi_) {
        std::cerr << "Already in MULTI mode\n" << std::endl;
        return 1;
    }
    in_multi_ = true;
    lua_script_ = "";
    command_num_ = 0;
    return 0;
}

void RedisClient::add(const std::string& command, const std::string& key, const std::vector<std::string>& args) {
    std::ostringstream oss;
    oss << "local r_" << command_num_ << " = redis.call('" << command << "', '" << prefix_+key;
    for (int i = 0; i < args.size(); ++i) {
        oss << "', '" << args[i];
    }
    oss << "')\n";
    lua_script_ += oss.str();
    command_num_++;
}

int RedisClient::exec(std::optional<std::vector<std::vector<std::string>>*> results) {
    if (!in_multi_) {
        std::cerr << "Not in MULTI mode\n" << std::endl;
        return 1;
    } else {
        in_multi_ = false;
    }
    
    if (command_num_ > 0 && results.has_value()) {
        lua_script_ += "return {r_0";
        for (int i = 1; i < command_num_; ++i) {
            lua_script_ += ", r_" + std::to_string(i);
        }
        lua_script_ += "}";
    } else {
        lua_script_ += "return 'OK'";
    }
    //printf("Executing Lua script:\n%s\n", lua_script_.c_str());
    redisReply *reply = (redisReply*)redisCommand(context_, "EVAL %s 0", lua_script_.c_str());
    if (reply == NULL) {
        printf("Error executing Lua script: %s\n", context_->errstr);
        return 1;
    } else if (reply->type == REDIS_REPLY_ARRAY) {
        if (results.has_value()) {
            results.value()->resize(reply->elements);
            for (size_t i = 0; i < reply->elements; ++i) {
                redisReply *sub_reply = reply->element[i];
                if (sub_reply->type == REDIS_REPLY_ARRAY) {
                    (*results.value())[i].resize(sub_reply->elements);
                    for (size_t j = 0; j < sub_reply->elements; ++j) {
                        if (sub_reply->element[j]->type == REDIS_REPLY_NIL) {
                            (*results.value())[i][j] = "NIL";
                        } else if (sub_reply->element[j]->type == REDIS_REPLY_STRING) {
                            (*results.value())[i][j] = sub_reply->element[j]->str;
                        } else {
                            printf("Unexpected reply type for sub-reply: %d\n", sub_reply->element[j]->type);
                            return 1;
                        }
                    }
                }
            }
        } else {
            printf("No optional parameter is not provided for results\n");
        }
    }
    freeReplyObject(reply);
    return 0;
}

int RedisClient::smembers(const std::string& key, std::vector<std::string>& members) {
    redisReply *reply = (redisReply*)redisCommand(context_, "SMEMBERS %s", (prefix_+key).c_str());
    if (reply == NULL) {
        printf("Error executing SMEMBERS command: %s\n", context_->errstr);
        return 1;
    } else if (reply->type != REDIS_REPLY_ARRAY) {            
        printf("Unexpected reply type for SMEMBERS: %d\n", reply->type);
        freeReplyObject(reply); 
        return 1;
    } else {
        members.resize(reply->elements);
        for (size_t i = 0; i < reply->elements; ++i) {
            members[i] = reply->element[i]->str;
        }
        freeReplyObject(reply); 
    }

    return 0;
}

int RedisClient::sadd(const std::string& key, const std::string& member) {
    redisReply *reply = (redisReply*)redisCommand(context_, "SADD %s %s", (prefix_+key).c_str(), member.c_str());
    if (reply == NULL) {
        printf("Error executing SADD command: %s\n", context_->errstr);
        return 1;
    }
    return 0;
}


