#include "MapReduceFramework.h"
#include <pthread.h>
#include <iostream>
#include <atomic>
#include <algorithm>
#include "Barrier.h"

struct Atomics
{
    std::atomic<int>* map_phase_atomic_counter;
    std::atomic<int>* shuffle_phase_atomic_counter;
    std::atomic<int>* reduce_phase_atomic_counter;
    std::atomic<int>* inter_pairs_atomic_counter;
    std::atomic<int>* progress_atomic_counter;
};

struct Mutexes
{
    pthread_mutex_t* emit3_mutex;
    pthread_mutex_t* stage_update_mutex;
    pthread_mutex_t* wait_mutex;
};

struct ThreadContext
{
    int ThreadId;
    const MapReduceClient* client;
    const InputVec* input_vec;
    OutputVec* output_vec;
    IntermediateVec* inter_vec;
    Atomics * current_job_atomic_counters;
    Mutexes * current_job_mutexes;
    Barrier * barrier;
    JobState * current_job_state;
    std::vector<IntermediateVec *> * inters_vec_of_vec;
    std::vector<IntermediateVec *> * shuffled_vec_of_vec;
};

struct Job
{
    const InputVec* input_vec;
    JobState * state;
    pthread_t ** current_job_threads;
    ThreadContext ** contexts;
    int num_of_threads;
    int already_called_pthread_join;
    std::vector<IntermediateVec *> * inters_vec_of_vec;
    std::vector<IntermediateVec *> * shuffled_vec_of_vec;
    Atomics * current_job_atomics;
    Mutexes * current_job_mutexes;
    Barrier * barrier;
};



void emit2 (K2* key, V2* value, void* context)
{
    auto tc = (ThreadContext*)context;
    (*(tc->current_job_atomic_counters->inter_pairs_atomic_counter))++;
    tc->inter_vec->push_back(IntermediatePair(key, value));
}


void emit3 (K3* key, V3* value, void* context)
{
    auto tc = (ThreadContext*)context;
    if(pthread_mutex_lock(tc->current_job_mutexes->emit3_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    tc->output_vec->push_back(OutputPair(key, value));
    if(pthread_mutex_unlock(tc->current_job_mutexes->emit3_mutex) != 0)
    {
        {
            std::cout << "system error: mutex unlock failed\n";
            exit(1);
        }
    }
}


bool sort_by_key(const IntermediatePair& pair1, const IntermediatePair& pair2)
{
    return *pair1.first < *pair2.first;
}

void sort_stage(ThreadContext* thread_context)
{
    auto intermediate_vec = thread_context->inter_vec;
    if(intermediate_vec->size()>1)
    {
        std::sort(intermediate_vec->begin(), intermediate_vec->end(),sort_by_key);
    }
}

void shuffle_stage(ThreadContext* thread_context)
{
    while(*thread_context->current_job_atomic_counters->progress_atomic_counter < *thread_context->current_job_atomic_counters->inter_pairs_atomic_counter)
    {
        IntermediatePair max_pair = IntermediatePair(nullptr, nullptr);
        for(auto inter_vec_pointer : *thread_context->inters_vec_of_vec)
        {
            if(inter_vec_pointer->empty()) { continue;}
            if((max_pair.first == nullptr and max_pair.second == nullptr)|| *max_pair.first < *inter_vec_pointer->back().first)
            {
                max_pair = inter_vec_pointer->back();
            }
        }
        auto single_shuffle_vec = new IntermediateVec;
        thread_context->shuffled_vec_of_vec->push_back(single_shuffle_vec);
        for(auto inter_vec_pointer : *thread_context->inters_vec_of_vec)
        {
            if(inter_vec_pointer->empty()) { continue;}
            while(!inter_vec_pointer->empty() && (!(*inter_vec_pointer->back().first <*max_pair.first) && !(*max_pair.first <*inter_vec_pointer->back().first)))
            {
                IntermediatePair popped_pair = inter_vec_pointer->back();
                single_shuffle_vec->push_back(popped_pair);
                inter_vec_pointer->pop_back();
                (*(thread_context->current_job_atomic_counters->progress_atomic_counter ))++;
            }
        }
        (*(thread_context->current_job_atomic_counters->shuffle_phase_atomic_counter))++;
    }
}


void* thread_func(void* thread_context)
{
    auto tc = (ThreadContext*)thread_context;
    if(pthread_mutex_lock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    if(tc->ThreadId == 0)
    {
        tc->current_job_state->stage = MAP_STAGE;
    }
    if(pthread_mutex_unlock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex unlock failed\n";
        exit(1);
    }
    int old_map_value = (*(tc->current_job_atomic_counters->map_phase_atomic_counter))++;
    while(old_map_value < (int)tc->input_vec->size())
    {
        const K1* key = (*tc->input_vec)[old_map_value].first;
        const V1* value = (*tc->input_vec)[old_map_value].second;
        tc->client->map(key, value, thread_context);
        (*(tc->current_job_atomic_counters->progress_atomic_counter))++;
        old_map_value = (*(tc->current_job_atomic_counters->map_phase_atomic_counter))++;
    }
    sort_stage(tc);
    tc->barrier->barrier();
    if(pthread_mutex_lock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    if(tc->ThreadId == 0)
    {
        tc->current_job_state->stage = SHUFFLE_STAGE;
        tc->current_job_atomic_counters->progress_atomic_counter->store(0);
    }
    if(pthread_mutex_unlock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex unlock failed\n";
        exit(1);
    }
    tc->barrier->barrier();
    if(tc->ThreadId == 0)
    {
        shuffle_stage(tc);
    }
    tc->barrier->barrier();
    if(pthread_mutex_lock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    if(tc->ThreadId == 0)
    {
        tc->current_job_state->stage = REDUCE_STAGE;
        tc->current_job_atomic_counters->progress_atomic_counter->store(0);
    }
    if(pthread_mutex_unlock(tc->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout <<  "system error: mutex unlock failed\n";
        exit(1);
    }
    tc->barrier->barrier();
    int old_reduce_value = (*(tc->current_job_atomic_counters->reduce_phase_atomic_counter))++;
    while(old_reduce_value < *(tc->current_job_atomic_counters->shuffle_phase_atomic_counter))
    {
        int vec_size = (int)(*tc->shuffled_vec_of_vec)[old_reduce_value]->size();
        tc->client->reduce((*tc->shuffled_vec_of_vec)[old_reduce_value], thread_context);
        tc->current_job_atomic_counters->progress_atomic_counter->fetch_add(vec_size);
        old_reduce_value = (*(tc->current_job_atomic_counters->reduce_phase_atomic_counter))++;
    }
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    auto contexts = new ThreadContext*[multiThreadLevel];
    auto current_job_threads = new pthread_t*[multiThreadLevel];
    auto map_phase_atomic_counter = new std::atomic<int>(0);
    auto shuffle_phase_atomic_counter = new std::atomic<int>(0);
    auto reduce_phase_atomic_counter = new std::atomic<int>(0);
    auto inter_pairs_atomic_counter = new std::atomic<int>(0);
    auto progress_atomic_counter = new std::atomic<int>(0);
    auto barrier = new Barrier(multiThreadLevel);
    auto emit3_mutex = new pthread_mutex_t;
    if(pthread_mutex_init(emit3_mutex, nullptr) != 0)
    {
        std::cout << "system error: mutex init failed\n";
        exit(1);
    }
    auto stage_update_mutex = new pthread_mutex_t;
    if(pthread_mutex_init(stage_update_mutex, nullptr) != 0)
    {
        std::cout <<  "system error: mutex init failed\n";
        exit(1);
    }
    auto wait_mutex = new pthread_mutex_t;
    if(pthread_mutex_init(wait_mutex, nullptr) != 0)
    {
        std::cout << "system error: mutex init failed\n";
        exit(1);
    }
    auto current_job_atomic_counters = new Atomics{map_phase_atomic_counter,shuffle_phase_atomic_counter, reduce_phase_atomic_counter,
                                                   inter_pairs_atomic_counter, progress_atomic_counter};
    auto current_job_mutexes = new Mutexes{emit3_mutex, stage_update_mutex, wait_mutex};
    auto inters_vec_of_vec = new std::vector<IntermediateVec *>;
    auto shuffled_vec_of_vec = new std::vector<IntermediateVec*>;
    auto * current_job_state = new JobState{stage_t::UNDEFINED_STAGE, 0};
    Job * current_job = new Job{&inputVec,current_job_state, current_job_threads, contexts, multiThreadLevel,
                                false, inters_vec_of_vec,
                                shuffled_vec_of_vec, current_job_atomic_counters, current_job_mutexes, barrier};
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        auto inter_vec = new IntermediateVec;
        current_job->inters_vec_of_vec->push_back(inter_vec);
        contexts[i] = new ThreadContext{i, &client, &inputVec,
                                        &outputVec, inter_vec,
                                        current_job_atomic_counters, current_job_mutexes,
                                        barrier, current_job_state, inters_vec_of_vec, shuffled_vec_of_vec};
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        current_job_threads[i] = new pthread_t;
        if(pthread_create(current_job_threads[i], nullptr,
                          thread_func, contexts[i]) != 0)
        {
            std::cout << "system error: pthread create failed\n";
            exit(1);
        }
    }
    return current_job;
}

void waitForJob(JobHandle job)
{
    auto current_job = (Job*) job;
    if(pthread_mutex_lock(current_job->current_job_mutexes->wait_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    if(!(current_job->already_called_pthread_join))
    {
        for (int i = 0; i < current_job->num_of_threads; ++i) {
            pthread_join(*current_job->current_job_threads[i], nullptr);
        }
        current_job->already_called_pthread_join = true;
    }
    if(pthread_mutex_unlock(current_job->current_job_mutexes->wait_mutex) != 0)
    {
        std::cout <<  "system error: mutex unlock failed\n";
        exit(1);
    }
}

void getJobState(JobHandle job, JobState* state)
{
    auto current_job = (Job*) job;
    if(pthread_mutex_lock(current_job->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex lock failed\n";
        exit(1);
    }
    state->stage = current_job->state->stage;
    switch (state->stage) {
        case UNDEFINED_STAGE:
            state->percentage = 0;
            break;
        case MAP_STAGE:
            state->percentage = ((float)(*current_job->current_job_atomics->progress_atomic_counter)/(float)current_job->input_vec->size())*100;
            break;
        case SHUFFLE_STAGE:
            state->percentage = ((float)(*current_job->current_job_atomics->progress_atomic_counter)
                                 /(float)(*current_job->current_job_atomics->inter_pairs_atomic_counter))*100;
            break;
        case REDUCE_STAGE:
            state->percentage = ((float)(*current_job->current_job_atomics->progress_atomic_counter)
                                 /(float)(*current_job->current_job_atomics->inter_pairs_atomic_counter))*100;
            break;
    }
    if(pthread_mutex_unlock(current_job->current_job_mutexes->stage_update_mutex) != 0)
    {
        std::cout << "system error: mutex unlock failed\n";
        exit(1);
    }
}

void closeJobHandle(JobHandle job)
{

    waitForJob(job);
    auto current_job = (Job*) job;
//    deleting atomic
    delete current_job->current_job_atomics->map_phase_atomic_counter;
    delete current_job->current_job_atomics->shuffle_phase_atomic_counter;
    delete current_job->current_job_atomics->reduce_phase_atomic_counter;
    delete current_job->current_job_atomics->inter_pairs_atomic_counter;
    delete current_job->current_job_atomics->progress_atomic_counter;
    delete current_job->current_job_atomics;
//    deleting barrier
    delete current_job->barrier;
//    deleting mutexes
    if(pthread_mutex_destroy(current_job->current_job_mutexes->emit3_mutex) != 0)
    {
        std::cout << "system error: mutex destroy failed\n";
        exit(1);
    }
    if(pthread_mutex_destroy(current_job->current_job_mutexes->stage_update_mutex) != 0 )
    {
        std::cout << "system error: mutex destroy failed\n";
        exit(1);
    }
    if(pthread_mutex_destroy(current_job->current_job_mutexes->wait_mutex) != 0)
    {
        std::cout << "system error: mutex destroy failed\n";
        exit(1);
    }
    delete current_job->current_job_mutexes->emit3_mutex;
    delete current_job->current_job_mutexes->stage_update_mutex;
    delete current_job->current_job_mutexes->wait_mutex;
    delete current_job->current_job_mutexes;
//    deleting intermediate vectors
    for(auto inter : *current_job->inters_vec_of_vec)
    {
        delete inter;
    }
    delete current_job->inters_vec_of_vec;
//    deleting shuffled vectors
    for(auto shuffle : *current_job->shuffled_vec_of_vec)
    {
        delete shuffle;
    }
    delete current_job->shuffled_vec_of_vec;
//    deleting threadcontexts
    for(int i = 0; i< current_job->num_of_threads;i++)
    {
        delete current_job->contexts[i];
    }
    delete [] current_job->contexts;
//   deleting threads
    for(int i = 0; i< current_job->num_of_threads;i++)
    {
        delete current_job->current_job_threads[i];
    }
    delete [] current_job->current_job_threads;
    delete current_job->state;
    delete current_job;

}