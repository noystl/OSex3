// TODO: clean JobContext memory: use sem_destroy
// TODO: check what more sync problems could occur in map.

#include <string>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>

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
    int _numOfProcessedElements;
    pthread_mutex_t _processMutex;
    stage_t _stage;
    bool _doneShuffling;

    std::atomic<int> _atomicCounter;
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
                        _client(client), _numOfWorkers(multiThreadLevel), _numOfElements(inputVec->size()),
                        _numOfProcessedElements(0), _processMutex(PTHREAD_MUTEX_INITIALIZER),
                        _stage(UNDEFINED_STAGE), _doneShuffling(false),
                        _atomicCounter(0), _barrier(multiThreadLevel),
                        _inputVec(inputVec), _outputVec(outputVec),
                        _inputMutex(PTHREAD_MUTEX_INITIALIZER), _queueMutex(PTHREAD_MUTEX_INITIALIZER),
                        _outputMutex(PTHREAD_MUTEX_INITIALIZER)
    {
        // 2nd arg == 0 means that the semaphore can be used only by calling activity,
        // 3rd arg is the initial value of the semaphore
        if (sem_init(&_queueSizeSem, 0, 0))
        {
            std::cerr << "Error using sem_init." << std::endl;
            exit(1);
        }
    }
};


//----------------------------------------------- STATIC GLOBALS ------------------------------------------------//
static JobContext* jc;

/** should be non-negative and < numOfThreads */
static int shufflingThread = 3;

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
 * Unlocks the desired mutex.
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
 * This method updates the percentage in the jobState struct.
 * @param id
 * @param size
 * @param mutex
 */
static void updateProcess(int id, unsigned long processed, pthread_mutex_t* mutex){
    lock(mutex, id);
    jc->_numOfProcessedElements += processed;
    unlock(mutex, id);
}

/**
 * Compares between two intermediate pairs.
 * @param p1
 * @param p2
 * @return
 */
static bool intermediateComparator(const IntermediatePair& p1, const IntermediatePair& p2){
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
    K1* currElmKey;
    V1* currElmVal;
    bool notDone = false;

    // checks what is the index of the next element we should map:
    lock(&jc->_inputMutex, tc->_id);
    int old_value = (jc->_atomicCounter)++;

    if(old_value < (jc->_inputVec)->size()){
        notDone = true;
        currElmKey = jc->_inputVec->at(old_value).first;
        currElmVal = jc->_inputVec->at(old_value).second;
    }
    unlock(&jc->_inputMutex, tc->_id);

    // While there are elements to map, map them and keep the results in mapRes.
    while (notDone) {
        (jc->_client)->map(currElmKey, currElmVal, tc);
        updateProcess(tc->_id, 1, &jc->_processMutex);

        // Update JobState
        lock(&jc->_inputMutex, tc->_id);
        old_value = (jc->_atomicCounter)++;
        if(old_value < (jc->_inputVec)->size()){
            notDone = true;
            currElmKey = jc->_inputVec->at(old_value).first;
            currElmVal = jc->_inputVec->at(old_value).second;
        } else{
            notDone = false;
        }
        unlock(&jc->_inputMutex, tc->_id);
    }

    // Sorts the elements in the result of the Map stage:
    std::sort(tc->_mapRes.begin(), tc->_mapRes.end(), intermediateComparator);

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
    jc->_numOfElements = moreToGo;

    while (moreToGo > 0)
    {
        // finds the key for the "toReduce" vector:
        maxKey = nullptr;
        for (int j = 0; j < jc->_numOfWorkers; ++j)
        {
            if (!jc->_contexts[j]._mapRes.empty())
            {
                K2 *currKey = jc->_contexts[j]._mapRes.back().first;
                if (maxKey == nullptr || *maxKey < *currKey){
                    maxKey = currKey;
                }
            }
        }

        //pops all elements with the key, and adds them to the "toReduce" vector:
        for (int j = 0; j < jc->_numOfWorkers; ++j)
        {
            auto c = jc->_contexts[j]._mapRes;

            if (!jc->_contexts[j]._mapRes.empty() &&
                !(*maxKey < *(c.back().first)) &&
                !(*(c.back().first) < *maxKey))
            {
                toReduce.push_back(jc->_contexts[j]._mapRes.back());
                jc->_contexts[j]._mapRes.pop_back();
            }
        }

        //adds the vector to the queue & signal:
        lock(&jc->_queueMutex, shufflingThread);
        jc->_reducingQueue.push_back(toReduce);
        unlock(&jc->_queueMutex, shufflingThread);

        if(sem_post(&jc->_queueSizeSem))
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
        sem_wait(&jc->_queueSizeSem);

        lock(&jc->_queueMutex, tc->_id);

        //critical code:
        IntermediateVec pairs = jc->_reducingQueue.back();
        jc->_reducingQueue.pop_back();

        unlock(&jc->_queueMutex, tc->_id);

        (jc->_client)->reduce(&pairs ,tc);
        updateProcess(tc->_id, pairs.size(), &jc->_processMutex);
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
        jc->_stage = REDUCE_STAGE;

        lock(&jc->_processMutex, tc->_id);
        jc->_numOfProcessedElements = 0;
        unlock(&jc->_processMutex, tc->_id);

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
    for (int i = 0; i < jc->_numOfWorkers; ++i) {
        threadIndex = &((jc->_threads).at(i));
        contextIndex = &((jc->_contexts).at(i));
        if (pthread_create(threadIndex, nullptr, mapReduce, contextIndex))
        {
            std::cerr << "Error using pthread_create, on thread " << i << std::endl;
            exit(1);
        }
    }
    jc->_stage = MAP_STAGE;
}

//--------------------------------------------------PUBLIC METHODS--------------------------------------------------//
void emit2(K2 *key, V2 *value, void *context) {
    // Converting context to the right type:
    auto *tc = (ThreadContext *) context;

    // Inserting the map result to mapRes:
    tc->_mapRes.push_back(IntermediatePair(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    // Converting context to the right type:
    auto *tc = (ThreadContext *) context;
    lock(&jc->_outputMutex, tc->_id);

    //critical code:
    jc->_outputVec->push_back(OutputPair(key, value));

    unlock(&jc->_outputMutex, tc->_id);
}

void waitForJob(JobHandle job) {
    auto *context = (JobContext *) job;
    for (int i = 0; i < context->_numOfWorkers; ++i) {
        if(pthread_join(jc->_threads[i], nullptr)){
            std::cerr << "Error using pthread_join." << i << std::endl;
            exit(1);
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto *context = (JobContext *) job;
    state->percentage = (float)(context->_numOfProcessedElements * (100.0 / context->_numOfElements));
    state->stage = context->_stage;
}

void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto *context = (JobContext *) job;
    delete(context);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    //-----------INITIALIZE FRAMEWORK----------//
    //Initialize The JobContext:
    jc = new JobContext(&client, &inputVec, &outputVec, multiThreadLevel);

    //Initialize contexts for the threads:
    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext context{i};
        jc->_contexts.at(i) = context;
    }

    initThreads();

    return jc;
}