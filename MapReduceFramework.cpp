#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"

struct ThreadContext{
    std::atomic<int>* atomic_counter;
    const InputVec& inputVec;
    MapReduceClient& client;
    Barrier& barrier;
    IntermediateVec mapRes; // Keeps the results of the map stage.
};

/**
 * This is the function each thread runs in the beginning of the Map-Reduce process. It handles the Map and Sort
 * stages, and locks the running thread until all of the rest have finished.
 * @param arg
 * @return
 */
static void* mapSort(void* arg){
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

    // Sorts the elements in the result of the Map stage:
    std::sort(tc->mapRes.begin(), tc->mapRes.end());

    // Forces the thread to wait until all the others have finished the Sort phase.
    tc->barrier.barrier();

    return 0;
}


void emit2 (K2* key, V2* value, void* context){
    // Converting context to the right type:
    auto* tc = (ThreadContext*) context;

    // Inserting the map result to mapRes:
    tc->mapRes.push_back(IntermediatePair(key, value));

}