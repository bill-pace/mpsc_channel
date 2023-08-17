cmake_minimum_required(VERSION 3.26)
project(MpscChannel)

set(CMAKE_CXX_STANDARD 11)

add_executable(MpscChannel main.cpp)

add_executable(IntCounter int_counter.cpp)
