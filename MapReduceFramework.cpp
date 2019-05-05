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

//-------------------------------------------- USEFUL STRUCTS --------------------------------------------------//
/**
 * This struct holds the inner data of a thread.
 */
struct ThreadContext{
    int _id;
    IntermediateVec _mapRes; // Keeps the results of the map stage.
};

/**
 * This struct holds all parameters relevant to the job.
 */
struct JobContext{
    std::vector<pthread_t> _threads;
    std::vector<ThreadContext> _contexts;
    const MapReduceClient* _client;
    int _numOfWorkers;
    unsigned long _numOfElements;

    stage_t _stage;
    int _numOfProcessedElements;
    pthread_mutex_t _processMutex;

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
     * @param multiThreadLevel
     * @param jobContext
     */
    JobContext(const MapReduceClient* client,
                        const InputVec* inputVec, OutputVec* outputVec,
                        int multiThreadLevel):
                        _threads(multiThreadLevel), _contexts(multiThreadLevel),
                        _client(client), _numOfWorkers(multiThreadLevel),
                        _numOfElements(inputVec->size()), _numOfProcessedElements(0),
                        _stage(UNDEFINED_STAGE), _doneShuffling(false),
                        _atomicCounter(0), _barrier(multiThreadLevel), // Todo: can throw errors.
                        _inputVec(inputVec), _outputVec(outputVec)
    {
        // 2nd arg == 0 means that the semaphore can be used only by calling activity,
        // 3rd arg is the initial value of the semaphore
        if (sem_init(&_queueSizeSem, 0, 0))
        {
            std::cerr << "Error using sem_init." << std::endl;
            exit(1);
        }

        if (pthread_mutex_init(&_processMutex, nullptr) || pthread_mutex_init(&_inputMutex, nullptr)
            || pthread_mutex_init(&_queueMutex, nullptr) || pthread_mutex_init(&_outputMutex, nullptr))
        {
            std::cerr << "Error initializing Mutex." << std::endl;
            exit(1);
        }
    }

    /**
     * destructs this JobContext.
     */
    ~JobContext()
    {
        sem_destroy(&_queueSizeSem);

        if (pthread_mutex_destroy(&_processMutex)|| pthread_mutex_destroy(&_inputMutex)||
            pthread_mutex_destroy(&_queueMutex)|| pthread_mutex_destroy(&_outputMutex))
        {
            std::cerr << "Error destroying Mutex." << std::endl;
            exit(1);
        }
    }
};

//----------------------------------------------- STATIC GLOBALS ------------------------------------------------//
static JobContext* jc;

/** should be non-negative and < numOfThreads */
static int shufflingThread = 0;

//---------------------------------------------- STATIC FUNCTIONS ------------------------------------------------//

/**
 * Locks the desired mutex.
 * @param mutex
 * @param tid
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
 * @param mutex
 * @param tid
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
 * @param id
 * @param size
 * @param mutex
 */
static void updateProcess(unsigned long processed, pthread_mutex_t* mutex)
{
    lock(mutex);
    jc->_numOfProcessedElements += processed;
    unlock(mutex);
}

/**
 * Compares between two intermediate pairs.
 * @param p1
 * @param p2
 * @return
 */
static bool intermediateComparator(const IntermediatePair& p1, const IntermediatePair& p2)
{
    return *(p1.first) < *(p2.first);
}

/**
 * This is the function each thread runs in the beginning of the Map-Reduce process. It handles the Map and Sort
 * stages, and locks the running thread until all of the rest have finished.
 * @param tc
 */
void mapSort(void *threadContext)
{
    auto tc = (ThreadContext *)threadContext;
    InputPair currPair;
    K1* currElmKey = nullptr;
    V1* currElmVal = nullptr;
    bool notDone = false;

    // checks what is the index of the next element we should map:
    lock(&jc->_inputMutex);
    unsigned int old_value = (jc->_atomicCounter)++;

    if(old_value < (jc->_inputVec)->size()){
        notDone = true;
        currPair = (*(jc->_inputVec))[old_value];
        currElmKey = currPair.first;
        currElmVal = currPair.second;
    }
    unlock(&jc->_inputMutex);

    // While there are elements to map, map them and keep the results in mapRes.
    while (notDone) {
        (jc->_client)->map(currElmKey, currElmVal, tc);
        updateProcess(1, &jc->_processMutex);

        // Update JobState
        lock(&jc->_inputMutex);
        old_value = (jc->_atomicCounter)++;
        if(old_value < (jc->_inputVec)->size()){
            notDone = true;
            currPair = (*(jc->_inputVec))[old_value];
            currElmKey = currPair.first;
            currElmVal = currPair.second;
        } else{
            notDone = false;
        }
        unlock(&jc->_inputMutex);
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
}

/**
 * The shuffling functionality
 */
static void shuffle()
{
    K2 *maxKey;
    IntermediateVec toReduce;
    unsigned int moreToGo = 0;

    //set moreToGo & _numOfElements:
    for (int j = 0; j < jc->_numOfWorkers; ++j)
    {
        moreToGo += jc->_contexts[j]._mapRes.size();
    }
    jc->_numOfElements = moreToGo; // TODO: Verify with bar that this is safe, just in case.

    while (moreToGo > 0)
    {
        // finds the key for the "toReduce" vector:
        maxKey = nullptr;
        for (int j = 0; j < jc->_numOfWorkers; ++j)
        {
            if (!jc->_contexts[j]._mapRes.empty())
            {
                K2 *currKey = jc->_contexts[j]._mapRes.back().first;
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
            while (!jc->_contexts[j]._mapRes.empty() &&
                   !(*maxKey < *(jc->_contexts[j]._mapRes.back().first)) &&
                   !(*(jc->_contexts[j]._mapRes.back().first) < *maxKey))
            {
                try{
                    toReduce.push_back(jc->_contexts[j]._mapRes.back());
                }
                catch (std::bad_alloc &e)
                {
                    std::cerr << "system error: couldn't add the pair to the toReduce vector." << std::endl;
                    exit(1);
                }
                jc->_contexts[j]._mapRes.pop_back();
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
 */
static void reduce(ThreadContext *tc)
{
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
        updateProcess(pairs.size(), &jc->_processMutex);
    }
}

static void* mapReduce(void *arg)
{
    auto *tc = (ThreadContext *) arg;

    // ------mapSort:
    mapSort(tc);

    // ------shuffle:
    if (tc->_id == shufflingThread)
    {
        lock(&jc->_processMutex);

        //critical code:
        jc->_stage = REDUCE_STAGE;
        jc->_numOfProcessedElements = 0;

        unlock(&jc->_processMutex);
        shuffle();
        jc->_doneShuffling = true;
    }

    // ------reduce:
    reduce(tc);
}

/**
 * This function creates the mapping threads and activate them.
 * @param multiThreadLevel
 */
static void initThreads() {
    pthread_t *threadIndex;
    ThreadContext *contextIndex;

    lock(&jc->_processMutex);
    jc->_stage = MAP_STAGE;
    unlock(&jc->_processMutex);

    for (unsigned int i = 0; i < jc->_numOfWorkers; ++i) {
        threadIndex = &((jc->_threads)[i]);
        contextIndex = &((jc->_contexts)[i]);
        if (pthread_create(threadIndex, nullptr, mapReduce, contextIndex))
        {
            std::cerr << "Error using pthread_create, on thread " << i << std::endl;
            exit(1);
        }
    }
}

//--------------------------------------------------PUBLIC METHODS--------------------------------------------------//
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

void emit3(K3 *key, V3 *value, void *context) {
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
    auto *context = (JobContext *) job;

    // If there are no elements to proceed the job is as good as done, and need no waiting for.
    if(!context->_inputVec->empty()){
        for (int i = 0; i < context->_numOfWorkers; ++i) {
            if(pthread_join(jc->_threads[i], nullptr)){
                std::cerr << "Error using pthread_join." << i << std::endl;
                exit(1);
            }
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto *context = (JobContext *) job;
    if(!(context->_inputVec->empty())){
        lock(&jc->_processMutex);

        //critical code:
        state->percentage = (float)(context->_numOfProcessedElements * (100.0 / context->_numOfElements));
        state->stage = context->_stage;

        unlock(&jc->_processMutex);
    }

    else {
        // If there are no elements to proceed, the job is good as done:
        lock(&jc->_processMutex);

        //critical code:
        state->percentage = 100;
        state->stage = REDUCE_STAGE;

        unlock(&jc->_processMutex);
    }

}

void closeJobHandle(JobHandle job) {
    auto *context = (JobContext *) job;
    waitForJob(job);
    delete(context);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    assert(multiThreadLevel >= 0);

    //Initialize The JobContext:
    jc = new JobContext(&client, &inputVec, &outputVec, multiThreadLevel);

    if(!inputVec.empty()){
        //Initialize contexts for the threads:
        for (int i = 0; i < multiThreadLevel; ++i) {
            ThreadContext context{i};
            (jc->_contexts)[i] = context;
        }

        initThreads();
    }

    return jc;
}