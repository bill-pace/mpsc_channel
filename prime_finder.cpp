/**
 * @brief Demo use of multiple Channels together
 *
 * This demo finds all prime numbers less than 1,000,000,
 * sorts them into groups based on their last digit in
 * hex representation, and writes each group to its own
 * file in the working directory. The work of finding
 * the primes and grouping them by digit is repeated
 * across each of four threads, while a fifth monitors
 * the Receivers for each group's Channel and writes
 * lines to files as it finds lines to write.@n@n
 *
 * While the example is somewhat contrived, this demo
 * still showcases how a Channel or a collection of
 * Channels can synchronize access to one or more
 * logging or output files.
 */

#include "channel.h"

#include <array>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

using namespace std;

const int num_files = 16;

void find_primes(array<Transmitter<string>, num_files>);
void write_primes(array<Receiver<string>, num_files>);

int main() {
    array<Transmitter<string>, num_files> txs;
    array<Receiver<string>, num_files> rxs;
    for (unsigned i = 0; i < num_files; ++i) {
        auto tx_rx = open_channel<string>();
        txs[i] = std::move(tx_rx.first);
        rxs[i] = std::move(tx_rx.second);
    }

    auto writer_thread = thread { write_primes, std::move(rxs) };

    const unsigned num_finder_threads = 4;
    thread finders[num_finder_threads];
    for (auto & finder : finders) {
        finder = thread { find_primes, txs };
    }

    for (auto & tx : txs) {
        tx.close();
    }

    for (auto & finder : finders) {
        finder.join();
    }
    writer_thread.join();
    cout << "All threads complete!" << endl;
    return 0;
}

void find_primes(array<Transmitter<string>, num_files> txs) {
    // 0 and 1 known not prime, 2 known prime, everything above 2 candidate
    const unsigned num_primes = 1000000;
    auto * possible_primes = new bool[num_primes];
    possible_primes[0] = false;
    possible_primes[1] = false;
    for (unsigned i = 2; i < num_primes; ++i) {
        possible_primes[i] = true;
    }

    // sieve of Eratosthenes with no optimizations, just as a sample
    for (unsigned candidate = 2; candidate < num_primes; ++candidate) {
        if (possible_primes[candidate]) {
            // found prime number, send string down matching channel and set all multiples to false
            unsigned last_four_bits = candidate & 0xf;
            stringstream s {};
            s << "Thread " << this_thread::get_id() << " found prime number " << candidate
              << " that ends with 0x" << hex << last_four_bits << dec;
            txs[last_four_bits].send(s.str());

            for (unsigned multiple = candidate * 2; multiple < num_primes; multiple += candidate) {
                possible_primes[multiple] = false;
            }
        }
    }

    delete [] possible_primes;
}

void write_primes(array<Receiver<string>, num_files> rxs) {
    ofstream outfiles[num_files];
    for (unsigned i = 0; i < num_files; ++i) {
        stringstream filename {};
        filename << "ends_with_" << i << ".primes";
        outfiles[i] = ofstream { filename.str() };
    }

    bool any_channel_open { true };
    while (any_channel_open) {
        any_channel_open = false;
        for (unsigned i = 0; i < num_files; ++i) {
            auto & rx = rxs[i];
            if (rx.is_valid()) {
                bool channel_open;
                string s;
                if (rx.try_receive(s, channel_open)) {
                    outfiles[i] << s << endl;
                    any_channel_open = true;
                } else if (channel_open) {
                    any_channel_open = true;
                } else {
                    rx.close();
                }
            }
        }
    }
}
