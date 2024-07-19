cmake_minimum_required(VERSION 3.10)

# 项目名称
project(test_scheduler)

# 设置C++标准
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 查找hiredis库
#find_package(hiredis REQUIRED)
include_directories(/usr/local/include)
link_directories(/usr/local/lib)

# 添加可执行文件
file(GLOB SRC_FILES *.cpp)
message(STATUS "SRC_FILES: ${SRC_FILES}")
add_executable(test_scheduler ${SRC_FILES})

# 链接hiredis库
target_link_libraries(test_scheduler PRIVATE hiredis)
