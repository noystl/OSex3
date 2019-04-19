cmake_minimum_required(VERSION 3.13)
project(Ex3)

set(CMAKE_CXX_STANDARD 11)

add_executable(Ex3 MapReduceClient.cpp MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h)