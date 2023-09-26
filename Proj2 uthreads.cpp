#include "uthreads.h"
#include <iostream>
#include <list>
#include <map>
#include <array>
#include <algorithm>
#include <csetjmp>
#include <signal.h>
#include <sys/time.h>


#define JB_SP 6
#define JB_PC 7

// typedefs
typedef unsigned long address_t;

// thread struct
struct thread{
    int id;
    char * stack;
    int thread_quantums;
};

// states
std::list<int> ready_lst;
std::list<int> blocked_lst;
int running_thread;

// structures
std::array<thread*, MAX_THREAD_NUM> active_threads;
std::map<int, int> sleeping_threads;
std::list<int> available_id;
sigjmp_buf env[MAX_THREAD_NUM];

// global variables
int general_quantum = 0;
int total_quantums = 0;
struct sigaction sa = {0};
struct itimerval timer;
sigset_t mask_set;

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
            : "=g" (ret)
            : "0" (addr));
    return ret;
}

/**
 * deletes thread
 * @return no return
*/
void delete_thread(thread ** trd)
{
    if((*trd)->id != 0)
    {
        delete[] (*trd)->stack;
    }
    delete *trd;
}

/**
 * deletes all threads
 * @return no return
*/
void delete_library()
{
    for(auto active_thread : active_threads)
    {
        if(active_thread != nullptr)
        {
            delete_thread(&active_thread);
        }
    }
}

/**
 * blocks/unblocks sigvtalrm
 * @return no return
*/
void mask_sigvtalrm(int state)
{
    if(sigprocmask(state, &mask_set, nullptr)<0)
    {
        delete_library();
        std::cerr<<"system error: sigprocmask error\n";
        exit(1);
    }
}

/**
 * sets quantum to the given time
 * @return no return
*/
void set_quantum()
{
    // Configure the timer to expire after general_quantum sec... */
    timer.it_value.tv_sec = general_quantum/1000000;        // first time interval, seconds part
    timer.it_value.tv_usec = general_quantum%1000000;        // first time interval, microseconds part
    timer.it_interval.tv_sec = 0;    // following time intervals, seconds part
    timer.it_interval.tv_usec = 0;    // following time intervals, microseconds part
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr))
    {
        delete_library();
        std::cerr<<"system error: setitimer error\n";
        exit(1);
    }
}

/**
 * creates new thread
 * @return new thread
*/
thread * setup_thread(thread_entry_point entry_point, int tid)
{
    char * stack = new (std::nothrow) char [STACK_SIZE];
    if(stack == nullptr)
    {
        delete_library();
        std::cerr<<"system error: no memory space\n";
        exit(1);
    }
    address_t sp = (address_t) stack + STACK_SIZE - sizeof(address_t);
    auto pc = (address_t) entry_point;
    sigsetjmp(env[tid], 1);
    (env[tid]->__jmpbuf)[JB_SP] = translate_address(sp);
    (env[tid]->__jmpbuf)[JB_PC] = translate_address(pc);
    if(sigemptyset(&env[tid]->__saved_mask)<0)
    {
        delete_library();
        std::cerr<<"system error: sigemptyset error\n";
        exit(1);
    }
    auto new_thread = new  (std::nothrow) thread;
    if(new_thread == nullptr)
    {
        delete_library();
        std::cerr<<"system error: no memory space\n";
        exit(1);
    }
    new_thread->id=tid;
    new_thread->stack=stack;
    new_thread->thread_quantums=0;
    return new_thread;
}

/**
 * update quantums of slepping threads
 * @return no return
*/
void update_sleeping_thread()
{
    auto it = sleeping_threads.begin();
    while (it != sleeping_threads.end())
    {
        (*it).second--;
        if((*it).second <= 0)
        {
            auto iter = std::find(blocked_lst.begin(), blocked_lst.end(),(*it).first);
            if(iter == blocked_lst.end())
            {
                ready_lst.push_back((*it).first);
            }
            it = sleeping_threads.erase(it);
        }
        else{++it;}
    }
}

/**
 * switching running threads
 * @return no return
*/
void context_switching_helper()
{
    // running thread already moved to ready/blocked state or been terminated!
    int ret_val = 0;
    if(running_thread != -1)
    {
        ret_val = sigsetjmp(env[running_thread], 1); /* saves running thread env before removal*/
    }
    if(ret_val == 0 )
    {
        running_thread= ready_lst.front();
        ready_lst.pop_front();
        active_threads[running_thread]->thread_quantums++;
        total_quantums++;
        update_sleeping_thread();
        set_quantum();
        siglongjmp(env[running_thread],1);
    }
}

/**
 * pushes the running thread to end of ready list and doing a context switch
 * @return no return
*/
void context_switching(int sig)
{
    mask_sigvtalrm(SIG_BLOCK);
    ready_lst.push_back(running_thread);
    context_switching_helper();
    mask_sigvtalrm(SIG_UNBLOCK);
}

/**
 * initialize the signals to sigvtlarm
 * @return no return
*/
void initialize_mask_set()
{
    if(sigemptyset(&mask_set)<0)
    {
        std::cerr<<"system error: sigemptyset error\n";
        exit(1);
    }
    if(sigaddset(&mask_set, SIGVTALRM)<0)
    {
        std::cerr<<"system error: sigaddset error\n";
        exit(1);
    }
}

/**
 * initialize timer
 * @return no return
*/
void initialize_timer()
{
    sa.sa_handler = &context_switching;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0)
    {
        std::cerr<<"system error: sigaction error\n";
        exit(1);
    }
}

/**
 * creates the main thread
 * @return no return
*/
void initialize_main_thread()
{
    auto main_thread = new  (std::nothrow) thread;
    if(main_thread == nullptr)
    {
        std::cerr<<"system error: no memory space\n";
        exit(1);
    }
    main_thread->id = 0;
    main_thread->stack = nullptr;
    main_thread->thread_quantums=1;
    running_thread = main_thread->id;
    active_threads[0] = main_thread;
}

/**
 * @brief initializes the thread library.
 *
 * Once this function returns, the main thread (tid == 0) will be set as RUNNING. There is no need to
 * provide an entry_point or to create a stack for the main thread - it will be using the "regular" stack and PC.
 * You may assume that this function is called before any other thread library function, and that it is called
 * exactly once.
 * The input to the function is the length of a quantum in micro-seconds.
 * It is an error to call this function with non-positive quantum_usecs.
 *
 * @return On success, return 0. On failure, return -1.
*/
int uthread_init(int quantum_usecs)
{
    initialize_mask_set();
    if(quantum_usecs <= 0)
    {
        std::cerr<<"thread library error: negative quantum is not allowed\n";
        return -1;
    }
    general_quantum = quantum_usecs;
    total_quantums = 1;
    initialize_timer();
    for (int i=1; i<MAX_THREAD_NUM; i++)
    {
        available_id.push_back(i);
    }
    initialize_main_thread();
    set_quantum();
    return 0;
}


/**
 * @brief Creates a new thread, whose entry point is the function entry_point with the signature
 * void entry_point(void).
 *
 * The thread is added to the end of the READY threads list.
 * The uthread_spawn function should fail if it would cause the number of concurrent threads to exceed the
 * limit (MAX_THREAD_NUM).
 * Each thread should be allocated with a stack of size STACK_SIZE bytes.
 * It is an error to call this function with a null entry_point.
 *
 * @return On success, return the ID of the created thread. On failure, return -1.
*/
int uthread_spawn(thread_entry_point entry_point)
{
    mask_sigvtalrm(SIG_BLOCK);
    if (entry_point == nullptr)
    {
        std::cerr<<"thread library error: null entry point\n";
        return -1;
    }
    if(available_id.empty())
    {
        std::cerr<<"thread library error: reached max threads number\n";
        return -1;
    }
    auto min_val=  std::min_element(available_id.begin(), available_id.end());
    auto new_thread = setup_thread(entry_point, *min_val);
    available_id.erase(min_val);
    ready_lst.push_back(new_thread->id);
    active_threads[new_thread->id] = new_thread;
    mask_sigvtalrm(SIG_UNBLOCK);
    return new_thread->id;
}

/**
 * @brief Terminates the thread with ID tid and deletes it from all relevant control structures.
 *
 * All the resources allocated by the library for this thread should be released. If no thread with ID tid exists it
 * is considered an error. Terminating the main thread (tid == 0) will result in the termination of the entire
 * process using exit(0) (after releasing the assigned library memory).
 *
 * @return The function returns 0 if the thread was successfully terminated and -1 otherwise. If a thread terminates
 * itself or the main thread is terminated, the function does not return.
*/
int uthread_terminate(int tid)
{
    mask_sigvtalrm(SIG_BLOCK);

    // main thread is terminated:
    if(tid == 0)
    {
        delete_library();
        exit(0);
    }

    // thread terminate itself:
    if(tid == running_thread)
    {
        available_id.push_back(tid);
        delete_thread(&active_threads[running_thread]);
        active_threads[tid] = nullptr;
        running_thread = -1;
        context_switching_helper();
        mask_sigvtalrm(SIG_UNBLOCK);
    }

    // terminate thread if exists:
    auto it=ready_lst.begin();
    for (; it != ready_lst.end(); ++it)
    {
        if((*it) == tid)
        {
            ready_lst.erase(it);
            available_id.push_back(tid);
            delete_thread(&(active_threads[tid]));
            active_threads[tid] = nullptr;
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    it=blocked_lst.begin();
    for (; it != blocked_lst.end(); ++it)
    {
        if((*it) == tid)
        {
            blocked_lst.erase(it);
            available_id.push_back(tid);
            delete_thread(&(active_threads[tid]));
            active_threads[tid] = nullptr;
            if(sleeping_threads.find(tid)!=sleeping_threads.end())
            {
                sleeping_threads.erase(tid);
            }
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    auto iter = sleeping_threads.begin();
    for(;iter != sleeping_threads.end();it++)
    {
        if(iter->first == tid)
        {
            sleeping_threads.erase(tid);
            available_id.push_back(tid);
            delete_thread(&active_threads[tid]);
            active_threads[tid] = nullptr;
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    std::cerr<<"thread library error: there is no thread with the given tid\n";
    return -1;
}


/**
 * @brief Blocks the thread with ID tid. The thread may be resumed later using uthread_resume.
 *
 * If no thread with ID tid exists it is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision should be made. Blocking a thread in
 * BLOCKED state has no effect and is not considered an error.
 *
 * @return On success, return 0. On failure, return -1.
*/
int uthread_block(int tid)
{
    mask_sigvtalrm(SIG_BLOCK);
    if(tid == 0)
    {
        std::cerr<<"thread library error: cant block the main thread\n"<<std::endl;
        return -1;
    }
    auto iter = ready_lst.begin();
    for(;iter != ready_lst.end(); iter++)
    {
        if((*iter) == tid) {
            blocked_lst.push_back((tid));
            ready_lst.erase(iter);
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    if(running_thread == tid)
    {
        blocked_lst.push_back(running_thread);
        context_switching_helper();
        mask_sigvtalrm(SIG_UNBLOCK);
        return 0;
    }
    iter = blocked_lst.begin();
    for(;iter != blocked_lst.end(); iter++)
    {
        if ((*iter)== tid)
        {
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    auto it = sleeping_threads.find(tid);
    if (it != sleeping_threads.end())
    {
        blocked_lst.push_back(it->first);
        mask_sigvtalrm(SIG_UNBLOCK);
        return 0;
    }
    std::cerr<<"thread library error: there is no thread with the given tid\n";
    return -1;
}


/**
 * @brief Resumes a blocked thread with ID tid and moves it to the READY state.
 *
 * Resuming a thread in a RUNNING or READY state has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 *
 * @return On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid)
{
    mask_sigvtalrm(SIG_BLOCK);
    if (running_thread == tid)
    {
        mask_sigvtalrm(SIG_UNBLOCK);
        return 0;
    }
    if(sleeping_threads.find(tid)!=sleeping_threads.end())
    {
        auto it = blocked_lst.begin();
        for(;it != blocked_lst.end(); it++)
        {
            if((*it) == tid)
            {
                blocked_lst.erase(it);
                break;
            }
        }
        mask_sigvtalrm(SIG_UNBLOCK);
        return 0;
    }
    auto iter = blocked_lst.begin();
    for(;iter != blocked_lst.end(); iter++)
    {
        if((*iter) == tid) {
            ready_lst.push_back((tid));
            blocked_lst.erase(iter);
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    iter = ready_lst.begin();
    for(;iter != ready_lst.end(); iter++)
    {
        if((*iter) == tid)
        {
            mask_sigvtalrm(SIG_UNBLOCK);
            return 0;
        }
    }
    std::cerr<<"thread library error: there is no thread with the given tid\n";
    return -1;
}

/**
 * @brief Blocks the RUNNING thread for num_quantums quantums.
 *
 * Immediately after the RUNNING thread transitions to the BLOCKED state a scheduling decision should be made.
 * After the sleeping time is over, the thread should go back to the end of the READY queue.
 * If the thread which was just RUNNING should also be added to the READY queue, or if multiple threads wake up
 * at the same time, the order in which they're added to the end of the READY queue doesn't matter.
 * The number of quantums refers to the number of times a new quantum starts, regardless of the reason. Specifically,
 * the quantum of the thread which has made the call to uthread_sleep isn’t counted.
 * It is considered an error if the main thread (tid == 0) calls this function.
 *
 * @return On success, return 0. On failure, return -1.
*/
int uthread_sleep(int num_quantums)
{
    mask_sigvtalrm(SIG_BLOCK);
    if(num_quantums == 0)
    {
        mask_sigvtalrm(SIG_UNBLOCK);
        return 0;
    }
    if(num_quantums < 0)
    {
        std::cerr<<"thread library error: negative number of quantums\n";
        return -1;
    }
    if(running_thread == 0)
    {
        std::cerr<<"thread library error: main thread can't call uthread_sleep\n";
        return -1;
    }
    sleeping_threads[running_thread] = num_quantums;
    context_switching_helper();
    mask_sigvtalrm(SIG_UNBLOCK);
    return 0;
}


/**
 * @brief Returns the thread ID of the calling thread.
 *
 * @return The ID of the calling thread.
*/
int uthread_get_tid()
{
    return running_thread;
}


/**
 * @brief Returns the total number of quantums since the library was initialized, including the current quantum.
 *
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number should be increased by 1.
 *
 * @return The total number of quantums.
*/
int uthread_get_total_quantums()
{
    return total_quantums;
}


/**
 * @brief Returns the number of quantums the thread with ID tid was in RUNNING state.
 *
 * On the first time a thread runs, the function should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state when this function is called, include
 * also the current quantum). If no thread with ID tid exists it is considered an error.
 *
 * @return On success, return the number of quantums of the thread with ID tid. On failure, return -1.
*/
int uthread_get_quantums(int tid)
{
    mask_sigvtalrm(SIG_BLOCK);
    if (active_threads[tid] != nullptr)
    {
        mask_sigvtalrm(SIG_UNBLOCK);
        return active_threads[tid]->thread_quantums;
    }
    else
    {
        std::cerr<<"thread library error: there is no thread with the given tid\n";
        return -1;
    }
}