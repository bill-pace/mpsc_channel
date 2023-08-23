#include "channel.h"

#include <iostream>
#include <thread>
#include <sstream>

using namespace std;

void send_task(const std::string &, Transmitter<std::string>);
void receive_task(Receiver<std::string> &&);

int main() {
    auto tx_rx = open_channel<std::string>();
    thread receiver_thread { receive_task, std::move(tx_rx.second) };

    thread transmitter_threads[10];
    for (size_t i = 0; i < 10; ++i) {
        stringstream name{};
        name << "Thread " << i;
        transmitter_threads[i] = thread(send_task, name.str(), tx_rx.first);
    }

    for (auto &thread: transmitter_threads) {
        thread.join();
    }
    tx_rx.first.close();

    receiver_thread.join();
}

void send_task(const std::string & name, Transmitter<std::string> tx) {
    string message { name + " says hello!" };
    tx.send(message);
}

void receive_task(Receiver<std::string> && rx) {
    bool channel_open { true };
    string msg {};
    while (channel_open) {
        while (rx.wait_receive(msg, channel_open)) {
            cout << msg << endl;
        }
    }
}
