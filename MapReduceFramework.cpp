#include "MapReduceFramework.h"
#include "Barrier.h"

struct ThreadContext {
    std::atomic<int>* atomic_counter;
    Barrier* barrier;
};

void* mapSort(void* arg){

}
