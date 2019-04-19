#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"

struct ThreadContext {
    std::atomic<int>* atomic_counter;
    std::vector<std::pair<K2*, V2*>>& mapRes; // Keeps the results of the map stage.
    const InputVec& inputVec;
    MapReduceClient& client;
    Barrier& barrier;
};

/**
 * This is the function each thread runs in the beginning of the Map-Reduce process. It handles the Map and Sort
 * stages, and locks the running thread until all of the rest have finished.
 * @param arg
 * @return
 */
void* mapSort(void* arg){
    auto* tc = (ThreadContext*) arg;
    K1* currElmKey;
    V1* currElmVal;

    // checks what is the index of the next element we should map:
    int old_value = (*(tc->atomic_counter))++;

    // While there are elements to map, map them and keep the results in mapRes.
    while(old_value < (tc->inputVec).size()){
        currElmKey = tc->inputVec.at(old_value).first;
        currElmVal = tc->inputVec.at(old_value).second;
        (tc->client).map(currElmKey, currElmVal, tc);
    }

    // Sort the elements in the Map stage result:

    // Forces the thread to wait until all the others have finished the Sort phase.
    tc->barrier.barrier();

    return 0;
}
