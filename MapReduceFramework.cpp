// TODO: clean JobContext memory.
// TODO: check what more sync problems could occur in map.

#include <string>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"


//-------------------------------------------- USEFUL STRUCTS --------------------------------------------------//
/**
 * This struct holds the inner data of a thread.
 */
struct ThreadContext {
    int id;
    std::atomic<int>* atomicCounter;
    const InputVec* inputVec;
    const MapReduceClient* client;
    Barrier* barrier;
    pthread_mutex_t* inputMutex; // Used to lock the input vector when needed.
    IntermediateVec mapRes; // Keeps the results of the map stage.
};

/**
 * This struct holds all parameters relevant to the job.
 */
struct JobContext {
    std::vector<pthread_t> threads;
    std::vector<ThreadContext> contexts;
    JobState state{};

    /**
     * A constructor for the JobContext struct.
     * @param multiThreadLevel
     * @param jobContext
     */
    JobContext(int multiThreadLevel) {
        std::vector<pthread_t> jobThreads(multiThreadLevel);
        std::vector<ThreadContext> jobContexts(multiThreadLevel);
        threads = jobThreads;
        contexts = jobContexts;
        state = {UNDEFINED_STAGE, 0};
    }
};


//----------------------------------------------- STATIC GLOBALS ------------------------------------------------//
static JobContext* jc;
int globInt = 0;
//---------------------------------------------- STATIC FUNCTIONS ------------------------------------------------//

/**
 * Locks the desired mutex.
 * @param mutex
 * @param tid
 */
static void lock(pthread_mutex_t *mutex, int tid){
    if (pthread_mutex_lock(mutex) != 0){
        std::cerr << "thread " << tid << "error on pthread_mutex_lock" << std::endl;
        exit(1);
    }
}

/**
 * Uocks the desired mutex.
 * @param mutex
 * @param tid
 */
static void unlock(pthread_mutex_t *mutex, int tid){
    if (pthread_mutex_unlock(mutex) != 0) {
        std::cerr << "thread " << tid << "error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }
}

/**
 * Compares between two intermediate pairs.
 * @param p1
 * @param p2
 * @return
 */
static bool intermediateComperator(const IntermediatePair& p1, const IntermediatePair& p2){
    return *(p1.first) < *(p2.first);
}

/**
 * This is the function each thread runs in the beginning of the Map-Reduce process. It handles the Map and Sort
 * stages, and locks the running thread until all of the rest have finished.
 * @param arg
 * @return
 */
static void* mapSort(void *arg) {
    auto *tc = (ThreadContext *) arg;
    K1* currElmKey;
    V1* currElmVal;
    bool notDone = false;

    // checks what is the index of the next element we should map:

    lock(tc->inputMutex, tc->id);
    int old_value = (*(tc->atomicCounter))++;

    if(old_value < (tc->inputVec)->size()){
        notDone = true;
        currElmKey = tc->inputVec->at(old_value).first;
        currElmVal = tc->inputVec->at(old_value).second;
    }
    unlock(tc->inputMutex, tc->id);

    // While there are elements to map, map them and keep the results in mapRes.
    while (notDone) {
        (tc->client)->map(currElmKey, currElmVal, tc);

        lock(tc->inputMutex, tc->id);
        old_value = (*(tc->atomicCounter))++;
        if(old_value < (tc->inputVec)->size()){
            notDone = true;
            currElmKey = tc->inputVec->at(old_value).first;
            currElmVal = tc->inputVec->at(old_value).second;
        } else{
            notDone = false;
        }
        unlock(tc->inputMutex, tc->id);
    }

    // Sorts the elements in the result of the Map stage:
    std::sort(tc->mapRes.begin(), tc->mapRes.end(), intermediateComperator);

    // Forces the thread to wait until all the others have finished the Sort phase.
    tc->barrier->barrier();
    return nullptr;
}


/**
 * This function creates the mapping threads and activate them.
 * @param multiThreadLevel
 * @param jc
 */
static void initMappingThreads(int multiThreadLevel) {
    pthread_t *threadIndex;
    ThreadContext *contextIndex;
    for (int i = 0; i < multiThreadLevel; ++i) {
        threadIndex = &((jc->threads).at(i));
        contextIndex = &((jc->contexts).at(i));
        pthread_create(threadIndex, NULL, mapSort, contextIndex);
    }
}


//--------------------------------------------------PUBLIC METHODS--------------------------------------------------//
void emit2(K2 *key, V2 *value, void *context) {
    // Converting context to the right type:
    auto *tc = (ThreadContext *) context;
    int forTestTid = tc->id;

    // Inserting the map result to mapRes:
    tc->mapRes.push_back(IntermediatePair(key, value));

}

void emit3(K3 *key, V3 *value, void *context) {

}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    //-----------INITIALIZE FRAMEWORK----------//
    //Initialize The JobContext:
    jc = new JobContext(multiThreadLevel);

    //Initialize contexts for the threads:
    std::atomic<int> atomicCounter(0);
    Barrier barrier(multiThreadLevel);
    pthread_mutex_t inputMutex = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext context{i, &atomicCounter, &inputVec, &client, &barrier, &inputMutex};
        jc->contexts.at(i) = context;
    }

    //-----------MAP & SORT------------//
    jc->state = {MAP_STAGE, 0};
    initMappingThreads(multiThreadLevel);
    //-----------SHUFFLE---------------//

    //-----------REDUCE----------------//

    // Wait for all of the threads to finish:
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(jc->threads[i], NULL);
    }
    return jc;
}
