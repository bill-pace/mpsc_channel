/**
 * @brief "Hello, world"-style demo
 *
 * This demo has ten threads send a greeting through a channel whose
 * receiving end is monitored by an 11th thread, writing each to
 * stdout. The simple demo shows how to have a receiver thread keep
 * track of whether a channel is still open so that it can terminate
 * a loop without missing any messages and clean up after itself
 * without deleting still-needed data.
 */

#include "channel.h"

#include <iostream>
#include <thread>
#include <sstream>

using namespace std;

void send_task(const string &, Transmitter<string>);
void receive_task(Receiver<string> &&);

int main() {
    auto tx_rx = open_channel<string>();
    thread receiver_thread { receive_task, std::move(tx_rx.second) };

    thread transmitter_threads[10];
    for (size_t i = 0; i < 10; ++i) {
        stringstream name{};
        name << "Thread " << i;
        transmitter_threads[i] = thread(send_task, name.str(), tx_rx.first);
    }

    tx_rx.first.close();
    for (auto &thread: transmitter_threads) {
        thread.join();
    }
    receiver_thread.join();
}

void send_task(const string & name, Transmitter<string> tx) {
    string message { name + " says hello!" };
    tx.send(std::move(message));
}

void receive_task(Receiver<string> && rx) {
    bool channel_open { true };
    string msg {};
    while (channel_open) {
        while (rx.wait_receive(msg, channel_open)) {
            cout << msg << endl;
        }
    }
}
