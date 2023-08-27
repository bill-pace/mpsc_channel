#include "channel.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

using namespace std;

void find_primes(vector<Transmitter<string>>);
void write_primes(vector<Receiver<string>>);

int main() {
    vector<Transmitter<string>> txs;
    vector<Receiver<string>> rxs;
    for (unsigned i = 0; i < 16; ++i) {
        auto tx_rx = open_channel<string>();
        txs.emplace_back(std::move(tx_rx.first));
        rxs.emplace_back(std::move(tx_rx.second));
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

void find_primes(vector<Transmitter<string>> txs) {
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
              << " that ends with 0x" << hex << last_four_bits << dec << endl;
            txs[last_four_bits].send(s.str());

            for (unsigned multiple = candidate * 2; multiple < num_primes; multiple += candidate) {
                possible_primes[multiple] = false;
            }
        }
    }

    delete [] possible_primes;
}

void write_primes(vector<Receiver<string>> rxs) {
    ofstream outfiles[16];
    for (unsigned i = 0; i < 16; ++i) {
        stringstream filename {};
        filename << "ends_with_" << i << ".primes";
        outfiles[i] = ofstream { filename.str() };
    }

    bool any_channel_open { true };
    while (any_channel_open) {
        any_channel_open = false;
        for (unsigned i = 0; i < 16; ++i) {
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
