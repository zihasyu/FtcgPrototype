#ifndef PRODUCECONSUMERQUEUE_H
#define PRODUCECONSUMERQUEUE_H

#include <boost/lockfree/queue.hpp>
#include <iostream>
#include <chrono>

template <class T>
class ProduceConsumerQueue
{
private:
    boost::lockfree::queue<T, boost::lockfree::fixed_sized<true>> lockFreeQueue_;

public:
    boost::atomic<bool> done_;
    ProduceConsumerQueue(uint32_t maxQueueSize) : lockFreeQueue_(maxQueueSize), done_(false)
    {
    }
    ~ProduceConsumerQueue()
    {
        bool flag = IsEmpty();
        if (flag)
        {
            delete lockFreeQueue_;
        }
        else
        {
            fprintf(stderr, "MessageQueue: Queue is not empty.\n");
            exit(EXIT_FAILURE);
        }
    }

    bool Push(const T &data)
    {
        while (!lockFreeQueue_.push(data))
        {
            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    bool Pop(T &data)
    {
        return lockFreeQueue_.pop(data);
    }

    bool IsEmpty()
    {
        return lockFreeQueue_.empty();
    }

    bool IsFull()
    {
        return lockFreeQueue_.full();
    }
};
#endif