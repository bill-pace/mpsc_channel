#include "channel.h"

#include <iostream>
#include <thread>

void send(Transmitter<size_t>, size_t, size_t);
void receive(Receiver<size_t> &&, size_t *);

int main() {
    const size_t length { 100000 };
    auto * received = new size_t[length];
    for (size_t idx = 0; idx < length; ++idx) {
        received[idx] = false;
    }

    auto tx_rx = open_channel<size_t>();
    auto rx_thread = std::thread { receive, std::move(tx_rx.second), received };

    const size_t num_threads = 10;
    const size_t increment = length / num_threads;
    std::thread tx_threads[num_threads];
    for (size_t thread_num = 0; thread_num < num_threads; ++thread_num) {
        tx_threads[thread_num] = std::thread {
            send,
            tx_rx.first,
            thread_num * increment,
            (thread_num + 1) * increment
        };
    }

    for (auto & thread : tx_threads) {
        thread.join();
    }
    tx_rx.first.close();
    rx_thread.join();

    size_t product { 1 };
    for (size_t idx = 0; idx < length; ++idx) {
        product *= received[idx];
    }
    std::cout << "The product of reception counts is " << product << std::endl;

    delete [] received;
}

void send(Transmitter<size_t> tx, size_t start, size_t stop) {
    for (; start < stop; ++start) {
        tx.send(start);
    }
}

void receive(Receiver<size_t> && rx, size_t * received) {
    bool channel_open { true };
    while (channel_open) {
        size_t i;
        while (rx.try_receive(i, channel_open)) {
            ++received[i];
        }
    }
}
