cmake_minimum_required(VERSION 3.15)
project(homework2)
link_libraries(ws2_32)
SET(GCC_NET_COMPILE_FLAGS "-lwsock32 -std=c++14")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_NET_COMPILE_FLAGS}")

add_executable(homework2 main.cpp hash_map_hpp.h)
add_executable(test single_node_test.cpp)