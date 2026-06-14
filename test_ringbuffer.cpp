#include "include/ringbuffer.h"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <intrin.h>
#include <thread>
#include <vector>

int main()
{
    isaacr::RingBuffer rb(1024);

    uint64_t total = 0;
    const int N = 1000;
    for (int i = 0; i < N; i++) {
        uint64_t x = i;  // varies at runtime
        uint64_t y = 0;

        unsigned int aux;
        uint64_t start = __rdtscp(&aux);
        rb.write(&x, sizeof(x));
        rb.read(&y, sizeof(y));
        total += __rdtscp(&aux) - start;

        assert(x == y);
    }
    std::cout << "Single Thread \nAvg cycles: " << total / N << "\n";

    // 1 producer, 1 consumer on separate threads
    const int MT_N = 1000000;
    isaacr::RingBuffer mt_rb(1024 * 64);

    std::thread producer([&]() {
        for (int i = 0; i < MT_N; i++) {
            uint64_t x = i;
            mt_rb.write(&x, sizeof(x));
        }
    });

    std::thread consumer([&]() {
        for (int i = 0; i < MT_N; i++) {
            uint64_t y;
            mt_rb.read(&y, sizeof(y));
        }
    });

    unsigned int aux;
    uint64_t mt_start = __rdtscp(&aux);
    producer.join();
    consumer.join();
    uint64_t mt_end = __rdtscp(&aux);

    std::cout << "Two Thread (1P/1C)\nAvg cycles per item: " << (mt_end - mt_start) / MT_N << "\n";

    // 2 producers, 2 consumers on separate threads
    const int MT_N2 = 1000000;
    isaacr::RingBuffer mt_rb2(1024 * 64);

    auto producer2 = [&]() {
        for (int i = 0; i < MT_N2 / 2; i++) {
            uint64_t x = i;
            mt_rb2.write(&x, sizeof(x));
        }
    };

    auto consumer2 = [&]() {
        for (int i = 0; i < MT_N2 / 2; i++) {
            uint64_t y;
            mt_rb2.read(&y, sizeof(y));
        }
    };

    std::vector<std::thread> threads2;
    threads2.emplace_back(producer2);
    threads2.emplace_back(producer2);
    threads2.emplace_back(consumer2);
    threads2.emplace_back(consumer2);

    uint64_t mt_start2 = __rdtscp(&aux);
    for (auto& t : threads2) t.join();
    uint64_t mt_end2 = __rdtscp(&aux);

    std::cout << "Four Thread (2P/2C)\nAvg cycles per item: " << (mt_end2 - mt_start2) / MT_N2 << "\n";
}