#include "channel.h"

#include <iostream>
#include <thread>

void send(Transmitter<size_t>, size_t, size_t);
void receive(Receiver<size_t> &&, size_t *);

int main(int argc, char ** argv) {
    if (argc != 3) return 1;

    size_t length;
    std::string arg = argv[1];
    try {
        std::size_t pos;
        length = std::stoi(arg, &pos);
        if (pos < arg.size()) {
            std::cerr << "Trailing characters after number: " << arg << '\n';
        }
    } catch (std::invalid_argument const &ex) {
        std::cerr << "Invalid number: " << arg << '\n';
    } catch (std::out_of_range const &ex) {
        std::cerr << "Number out of range: " << arg << '\n';
    }

    size_t num_threads;
    arg = argv[2];
    try {
        std::size_t pos;
        num_threads = std::stoi(arg, &pos);
        if (pos < arg.size()) {
            std::cerr << "Trailing characters after number: " << arg << '\n';
        }
    } catch (std::invalid_argument const &ex) {
        std::cerr << "Invalid number: " << arg << '\n';
    } catch (std::out_of_range const &ex) {
        std::cerr << "Number out of range: " << arg << '\n';
    }

    auto * received = new size_t[length];
    for (size_t idx = 0; idx < length; ++idx) {
        received[idx] = 0;
    }

    auto tx_rx = open_channel<size_t>();
    auto rx_thread = std::thread { receive, std::move(tx_rx.second), received };

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
    size_t i;
    if (rx.wait_receive(i, channel_open)) {
        ++received[i];
    }
    while (channel_open) {
        while (rx.try_receive(i, channel_open)) {
            ++received[i];
        }
    }
}
