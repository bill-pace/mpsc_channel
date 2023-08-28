/**
 * @brief Demo the lack of data races
 *
 * This demo showcases how quickly a Channel can move data
 * across threads without losing any messages. When called
 * from the command line, it takes two arguments: how many
 * integers to transmit across a Channel<size_t>, and how
 * many transmitter threads to create. Note that the first
 * number should be evenly divisible by the second number.
 * @n@n
 *
 * As numbers are transmitted, the receiving thread will
 * use them as indices to determine which count in a large
 * array to increment. After joining with the receiver
 * thread, the main thread will then calculate the product
 * of the array of counts, which should be one, and print
 * it to stdout. A result of zero would indicate that at
 * least one number was never transmitted whereas a result
 * greater than one would indicate that at least one
 * number was transmitted multiple times.@n@n
 *
 * As neither of these cases occur, this demo shows that a
 * Channel is free of data races. As this demo does very
 * little work outside of pushing numbers through a queue,
 * it can also be used to explore how different numbers of
 * threads work with the Channel.
 */

#include "channel.h"

#include <iostream>
#include <thread>

using namespace std;

void send(Transmitter<size_t>, size_t, size_t);
void receive(Receiver<size_t> &&, size_t *);

int main(int argc, char ** argv) {
    if (argc != 3) return 1;

    size_t length;
    string arg = argv[1];
    try {
        size_t pos;
        length = stoi(arg, &pos);
        if (pos < arg.size()) {
            cerr << "Trailing characters after number: " << arg << endl;
        }
    } catch (invalid_argument const &ex) {
        cerr << "Invalid number: " << arg << endl;
    } catch (out_of_range const &ex) {
        cerr << "Number out of range: " << arg << endl;
    }

    size_t num_threads;
    arg = argv[2];
    try {
        size_t pos;
        num_threads = stoi(arg, &pos);
        if (pos < arg.size()) {
            cerr << "Trailing characters after number: " << arg << endl;
        }
    } catch (invalid_argument const &ex) {
        cerr << "Invalid number: " << arg << endl;
    } catch (out_of_range const &ex) {
        cerr << "Number out of range: " << arg << endl;
    }

    auto * received = new size_t[length];
    for (size_t idx = 0; idx < length; ++idx) {
        received[idx] = 0;
    }

    auto tx_rx = open_channel<size_t>();
    auto rx_thread = thread { receive, std::move(tx_rx.second), received };

    const size_t increment = length / num_threads;
    thread tx_threads[num_threads];
    for (size_t thread_num = 0; thread_num < num_threads; ++thread_num) {
        tx_threads[thread_num] = thread {
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
    cout << "The product of reception counts is " << product << endl;

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
    while (channel_open) {
        while (rx.wait_receive(i, channel_open)) {
            ++received[i];
        }
    }
}
