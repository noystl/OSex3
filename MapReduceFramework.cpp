// TODO: We are exiting from a constructor - is that ok?

#include <string>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <cassert>
#include <unordered_map>

//-------------------------------------------- USEFUL STRUCTS --------------------------------------------------//


/**
 * This struct holds all parameters relevant to the thread.
 */
struct ThreadContext
{
    int _id;
    int _jid;
    pthread_t _thread;
    IntermediateVec _mapRes; // Keeps the results of the map stage.

    /**
     * constructs a new thread context object
     * @param tid: the thread's id
     * @param jid : the id of the job to which the thread in connected
     * @param threadObj: the thread
     */
    ThreadContext(int tid, int jid):_id(tid), _jid(jid){}
};

/**
 * This struct holds all parameters relevant to the job.
 */
struct JobContext{
    int _jid;

    std::vector<ThreadContext*> _contexts;
    const MapReduceClient* _client;
    int _numOfWorkers;
    long _numOfElements;

    stage_t _stage;
    int _numOfProcessedElements;
    pthread_mutex_t _stateMutex;

    bool _doneShuffling;

    std::atomic<unsigned int> _atomicCounter;
    Barrier _barrier;

    const InputVec* _inputVec;
    pthread_mutex_t _inputMutex; // Used to lock the input vector when needed.
    std::vector<IntermediateVec> _reducingQueue;
    sem_t _queueSizeSem;
    pthread_mutex_t _queueMutex; //Used to lock the jobs queue
    OutputVec* _outputVec;
    pthread_mutex_t _outputMutex; //Used to lock the output vector


     /**
      * A constructor for the JobContext struct.
      * @param jid: the job's id
      * @param client: the job's client
      * @param inputVec : the job's input
      * @param outputVec : the place for the job to output to.
      * @param multiThreadLevel: the job's multi thread level.
      */
    JobContext(int jid, const MapReduceClient* client,
                        const InputVec* inputVec, OutputVec* outputVec,
                        int multiThreadLevel):
                        _jid(jid),_contexts(multiThreadLevel),
                        _client(client), _numOfWorkers(multiThreadLevel),
                        _numOfElements(inputVec->size()),_stage(UNDEFINED_STAGE),
                        _numOfProcessedElements(0), _doneShuffling(false),
                        _atomicCounter(0), _barrier(multiThreadLevel),
                        _inputVec(inputVec), _outputVec(outputVec),
                        _stateMutex(PTHREAD_MUTEX_INITIALIZER),
                        _inputMutex(PTHREAD_MUTEX_INITIALIZER),
                        _queueMutex(PTHREAD_MUTEX_INITIALIZER),
                        _outputMutex(PTHREAD_MUTEX_INITIALIZER)
    {

        if (sem_init(&_queueSizeSem, 0, 0))
        {
            std::cerr << "Error using sem_init." << std::endl;
            exit(1);
        }

    }

    /**
     * destructs this JobContext.
     */
    ~JobContext()
    {
        sem_destroy(&_queueSizeSem);
    }
};



//----------------------------------------------- STATIC GLOBALS ------------------------------------------------//
/** next job Index */
static unsigned int nextIndex(0);

/** holds the library's jobs*/
static std::unordered_map<unsigned int, JobContext*> jobs;

/** locks the job's dictionary and index */
static pthread_mutex_t jobsVecMutex = PTHREAD_MUTEX_INITIALIZER; // TODO: how to initialize safely? When to destroy? Should we destroy it?

/** should be non-negative and < numOfThreads */
static int shufflingThread = 0;

//---------------------------------------------- STATIC FUNCTIONS ------------------------------------------------//

/**
 * Frees the of the map reduce
 */
static void freeLibMem(){

}

/**
 * Locks the desired mutex.
 * @param mutex: the mutex to lock
 */
static void lock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
    {
        std::cerr << "error on pthread_mutex_lock" << std::endl;
        exit(1);
    }
}

/**
 * Unlocks the desired mutex.
 * @param mutex: the mutex to unlock
 */
static void unlock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_unlock(mutex) != 0)
    {
        std::cerr << "error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }
}

/**
 * This method updates the percentage in the jobState struct.
 * @param jc: the job'x context
 * @param processed: the number of processed elements to update
 */
static void updateProcess(JobContext* jc, unsigned long processed)
{
    lock(&jc->_stateMutex);
    jc->_numOfProcessedElements += processed;
    unlock(&jc->_stateMutex);
}


/**
 * Compares between two intermediate pairs.
 * @param p1: An object of an intermediate type.
 * @param p2: An object of an intermediate type.
 * @return: 1 if p2 > p1, and zero otherwise.
 */
static bool intermediateComparator(const IntermediatePair& p1, const IntermediatePair& p2)
{
    return *(p1.first) < *(p2.first);
}

/**
 * This is the function each thread runs in the beginning of the Map-Reduce process. It handles the Map and Sort
 * stages, and locks the running thread until all of the rest have finished.
 * @param tc: A struct contains the inner state of a thread.
 */
static void* mapSort(ThreadContext * tc){
    JobContext *jc = jobs[tc->_jid];
    InputPair currPair;
    K1* currElmKey = nullptr;
    V1* currElmVal = nullptr;

    // checks what is the index of the next element we should map:
    unsigned int old_value = (jc->_atomicCounter)++;
    currPair = (*(jc->_inputVec))[old_value];
    currElmKey = currPair.first;
    currElmVal = currPair.second;

    // While there are elements to map, map them and keep the results in mapRes.
    while (old_value < (jc->_inputVec)->size()) {
        (jc->_client)->map(currElmKey, currElmVal, tc);
        updateProcess(jc, 1);

        old_value = (jc->_atomicCounter)++;
        currPair = (*(jc->_inputVec))[old_value];
        currElmKey = currPair.first;
        currElmVal = currPair.second;
    }

    // Sorts the elements in the result of the Map stage:
    try{
        std::sort(tc->_mapRes.begin(), tc->_mapRes.end(), intermediateComparator);
    }
    catch (std::bad_alloc &e)
    {
        std::cerr << "System Error: Sorting map results had failed." << std::endl;
        exit(1);
    }

    // Forces the thread to wait until all the others have finished the Sort phase.
    jc->_barrier.barrier();

    return 0;
}

/**
 * The shuffling functionality
 * @param tc A struct contains the inner data of a thread.
 */
static void shuffle(ThreadContext* tc)
{

    JobContext *jc = jobs[tc->_jid]; // TODO sys error
    K2 *maxKey;
    IntermediateVec toReduce;
    unsigned int moreToGo = 0;

    //set moreToGo & _numOfElements:
    for (int j = 0; j < jc->_numOfWorkers; ++j)
    {
        moreToGo += jc->_contexts[j]->_mapRes.size();
    }
    jc->_numOfElements = moreToGo;

    while (moreToGo > 0)
    {
        // finds the key for the "toReduce" vector:
        maxKey = nullptr;
        for (int j = 0; j < jc->_numOfWorkers; ++j)
        {
            if (!jc->_contexts[j]->_mapRes.empty())
            {
                K2 *currKey = jc->_contexts[j]->_mapRes.back().first;
                if (maxKey == nullptr || *maxKey < *currKey)
                {
                    maxKey = currKey;
                }
            }
        }

        //pops all elements with the key, and adds them to the "toReduce" vector:
        for (int j = 0; j < jc->_numOfWorkers; ++j)
        {
            assert (maxKey != nullptr);
            while (!jc->_contexts[j]->_mapRes.empty() &&
                   !(*maxKey < *(jc->_contexts[j]->_mapRes.back().first)) &&
                   !(*(jc->_contexts[j]->_mapRes.back().first) < *maxKey))
            {
                try{
                    toReduce.push_back(jc->_contexts[j]->_mapRes.back());
                }
                catch (std::bad_alloc &e)
                {
                    std::cerr << "system error: couldn't add the pair to the toReduce vector." << std::endl;
                    exit(1);
                }
                jc->_contexts[j]->_mapRes.pop_back();
            }
        }

        //adds the vector to the queue & signal:
        lock(&jc->_queueMutex);
        try
        {
            jc->_reducingQueue.push_back(toReduce);
        }
        catch (std::bad_alloc &e)
        {
            std::cerr << "system error: couldn't add the vector to the reducing queue." << std::endl;
            exit(1);
        }
        unlock(&jc->_queueMutex);

        if (sem_post(&jc->_queueSizeSem))
        {
            std::cerr << "Error using sem_post." << std::endl;
            exit(1);
        }
        moreToGo -= toReduce.size();
        toReduce.clear();
    }
}

/**
 * The reducing functionality
 * @param tc a struct contains the inner data of a thread.
 */
static void reduce(ThreadContext *tc)
{
    JobContext *jc = jobs[tc->_jid];
    while(!(jc->_doneShuffling && jc->_reducingQueue.empty()))
    {

        if (sem_wait(&jc->_queueSizeSem))
        {
            std::cerr << "Error using sem_wait." << std::endl;
            exit(1);
        }
        lock(&jc->_queueMutex);

        //critical code:
        IntermediateVec pairs = jc->_reducingQueue.back();
        jc->_reducingQueue.pop_back();

        unlock(&jc->_queueMutex);

        (jc->_client)->reduce(&pairs ,tc);
        updateProcess(jc, pairs.size());
    }
}

/**
 * This is the function that all of the threads of a job should run in order to preform the map reduce process.
 * @param arg A struct contains the inner data of a thread.
 * @return nullptr.
 */
static void* mapReduce(void *arg)
{
    auto *tc = (ThreadContext *) arg;
    JobContext *jc = jobs[tc->_jid];

    // ------mapSort:
    mapSort(tc);

    // ------shuffle:
    if (tc->_id == shufflingThread)
    {
        lock(&jc->_stateMutex);

        //critical code:

        jc->_stage = REDUCE_STAGE;
        jc->_numOfProcessedElements = 0;

        unlock(&jc->_stateMutex);
        shuffle(tc);
        jc->_doneShuffling = true;
    }
    // ------reduce:

    reduce(tc);

    return nullptr;
}

/**
 * This function creates the mapping threads and activate them.
 * @param jc A struct contains the inner data of a job.
 */
static void initThreads(JobContext* jc) {

    lock(&jc->_stateMutex);
    jc->_stage = MAP_STAGE;
    unlock(&jc->_stateMutex);

    for (int i = 0; i < jc->_numOfWorkers; ++i) {
        //Initialize Threads contexts:
        auto *tc = new ThreadContext(i, jc->_jid);
        (jc->_contexts)[i] = tc;

        if (pthread_create(&tc->_thread, nullptr, mapReduce, tc))
        {
            std::cerr << "Error using pthread_create, on thread " << i << std::endl;

            // TODO: Free memory and exit

            exit(1);
        }
    }
}

//--------------------------------------------------PUBLIC METHODS--------------------------------------------------//

/**
 * This function produces a (K2*,V2*) pair.The context can be used to get pointers into the framework’s variables and
 * data structures.
 * @param key: The key of an intermediate value.
 * @param value: The value of an intermediate value.
 * @param context: The context of the calling thread.
 */
void emit2(K2 *key, V2 *value, void *context) {
    // Converting context to the right type:
    auto *tc = (ThreadContext *) context;

    // Inserting the map result to mapRes:
    try{
        tc->_mapRes.push_back(IntermediatePair(key, value));
    }
    catch (std::bad_alloc &e)
    {
        std::cerr << "system error: couldn't add to the IntermediatePairs vector." << std::endl;
        exit(1);
    }
}

/**
 * This function produces a (K3*,V3*) pair.The context can be used to get pointers into the framework’s variables and
 * data structures.
 * @param key: The key of an intermediate value.
 * @param value: The value of an intermediate value.
 * @param context: The context of the calling thread.
 */
void emit3(K3 *key, V3 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    JobContext *jc = jobs[tc->_jid];

    // Converting context to the right type:
    lock(&jc->_outputMutex);

    //critical code:
    try{
        jc->_outputVec->push_back(OutputPair(key, value));
    }
    catch (std::bad_alloc &e)
    {
        std::cerr << "system error: couldn't to the output vector." << std::endl;
        exit(1);
    }
    unlock(&jc->_outputMutex);
}

void waitForJob(JobHandle job) {
    auto *jc = (JobContext *) job;

    // If there are no elements to proceed the job is as good as done, and needs no waiting for.
    if(!jc->_inputVec->empty()){
        for (int i = 0; i < jc->_numOfWorkers; ++i) {
            if(pthread_join(jc->_contexts[i]->_thread, nullptr)){
                std::cerr << "Error using pthread_join." << i << std::endl;
                exit(1);
            }
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto *jc = (JobContext *) job;
    if(!(jc->_inputVec->empty())){
        lock(&jc->_stateMutex);

        //critical code:
        state->percentage = (float)(jc->_numOfProcessedElements * (100.0 / jc->_numOfElements));
        state->stage = jc->_stage;

        unlock(&jc->_stateMutex);
    }

    else {
        // If there are no elements to proceed, the job is good as done:
        lock(&jc->_stateMutex);

        //critical code:
        state->percentage = 100;
        state->stage = REDUCE_STAGE;

        unlock(&jc->_stateMutex);
    }

}

void closeJobHandle(JobHandle job) {
    auto *jc = (JobContext *) job;
    waitForJob(job);

    for(int i = 0; i < jc->_numOfWorkers ; ++i){
        delete(jc->_contexts[i]);
    }
    delete(jc);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    assert(multiThreadLevel >= 0);

    //Initialize The JobContext:
    JobContext * jc = new JobContext((int)jobs.size(), &client, &inputVec, &outputVec, multiThreadLevel);

    //Add the new job to the job's vector:
    lock(&jobsVecMutex);
    unsigned int currIndex = (nextIndex)++;

    try{
        jobs.insert({currIndex, jc});

        //(std::pair<unsigned int, JobContext*>(currIndex, jc));
    }
    catch (std::bad_alloc &e)
    {
        std::cerr << "system error: couldn't add the new job." << std::endl;
        // TODO: Free memory and exit
        exit(1);
    }
    unlock(&jobsVecMutex);

    if(!inputVec.empty()){
        initThreads(jc);
    }

    return jc;
}


// GRAVE YARD
//void mapSort(ThreadContext * tc)
//{
//    JobContext *jc = jobs[tc->_jid];
//    InputPair currPair;
//    K1* currElmKey = nullptr;
//    V1* currElmVal = nullptr;
//    bool notDone = false;
//
//    // checks what is the index of the next element we should map:
//    lock(&jc->_inputMutex);
//    unsigned int old_value = (jc->_atomicCounter)++;
//
//    if(old_value < (jc->_inputVec)->size()){
//        notDone = true;
//        currPair = (*(jc->_inputVec))[old_value];
//        currElmKey = currPair.first;
//        currElmVal = currPair.second;
//    }
//    unlock(&jc->_inputMutex);
//
//    // While there are elements to map, map them and keep the results in mapRes.
//    while (notDone) {
//        (jc->_client)->map(currElmKey, currElmVal, tc);
//        updateProcess(jc, 1);
//
//        // Update JobState
//        lock(&jc->_inputMutex);
//        old_value = (jc->_atomicCounter)++;
//        if(old_value < (jc->_inputVec)->size()){
//            notDone = true;
//            currPair = (*(jc->_inputVec))[old_value];
//            currElmKey = currPair.first;
//            currElmVal = currPair.second;
//        } else{
//            notDone = false;
//        }
//        unlock(&jc->_inputMutex);
//    }
//
//    // Sorts the elements in the result of the Map stage:
//    try{
//        std::sort(tc->_mapRes.begin(), tc->_mapRes.end(), intermediateComparator);
//    }
//    catch (std::bad_alloc &e)
//    {
//        std::cerr << "System Error: Sorting map results had failed." << std::endl;
//        exit(1);
//    }
//
//    // Forces the thread to wait until all the others have finished the Sort phase.
//    jc->_barrier.barrier();
//}
