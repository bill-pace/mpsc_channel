//
// Created by bill on 8/12/23.
//

#ifndef MPSCCHANNEL_CHANNEL_H
#define MPSCCHANNEL_CHANNEL_H

#include <mutex>
#include <utility>

template<typename T> class Channel;
template<typename T> class Receiver;
template<typename T> class Transmitter;
template<typename T> std::pair<Transmitter<T>, Receiver<T>> open_channel();


// A ChannelNode is a single piece of data in the singly linked list.
// It has no public interface since instances should be managed entirely
// through the Channel instance that owns them.
template<typename T>
struct ChannelNode {
    friend Channel<T>;

    ChannelNode() = delete;

    ChannelNode(const ChannelNode &) = delete;

    ChannelNode& operator=(const ChannelNode &) = delete;

    ChannelNode(ChannelNode &&) = delete;

    ChannelNode& operator=(ChannelNode &&) = delete;

private:
    // make a new ChannelNode by taking ownership of a T instance
    explicit ChannelNode(T && data) : data(data), next(nullptr) {}

    T data;
    ChannelNode* next;
};

// A Channel is a FIFO queue with a queue_mutex lock. It has no public interface
// as it should be managed entirely through the transmitter and receiver
// instances that share ownership of it.
// Transmitters allow for pushing new data into their corresponding channels,
// while receivers allow for popping new data out for use elsewhere. The
// queue_mutex locks used in a channel allow for it to ensure thread-safe
// operations in both use cases, as well as supporting the dual owners in
// proper cleanup.
// A Channel owns the data contained within its nodes, and so both takes
// ownership of anything pushed to it and relinquishes ownership of
// anything popped from it. If the channel is not empty when deleted,
// everything still in the channel will be destructed.
template<typename T>
class Channel {
    friend class Transmitter<T>;
    friend class Receiver<T>;
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    Channel(const Channel&) = delete;

    Channel& operator=(const Channel&) = delete;

    Channel(Channel&&) = delete;

    Channel& operator=(Channel&&) = delete;

private:
    // channels are empty and open by default
    Channel() :
            queue_mutex(),
            tx_count_mutex(),
            first(nullptr),
            last(nullptr),
            tx_count(0),
            open(true)
    {}

    // destroy every owned node in the queue
    // only call from the close() method to ensure locked
    ~Channel() {
        ChannelNode<T> * next = first;
        while (next) {
            ChannelNode<T> * following = next->next;
            delete next;
            next = following;
        }
    }

    // add a new value to the queue, taking ownership of it
    void push(T&& new_value) {
        auto * new_node = new ChannelNode<T> { std::move(new_value) };
        const std::lock_guard<std::mutex> lock(queue_mutex);
        if (last) {
            last->next = new_node;
        } else {
            first = new_node;
        }
        last = new_node;
    }

    // attempt to remove a value from the queue
    // if the queue is empty, returns false without changing the output parameter
    // otherwise, moves the item from the first node into the output parameter and returns true
    bool try_pop(T& out, bool& is_open) {
        bool success = false;
        ChannelNode<T> * node = nullptr;
        {
            const std::lock_guard<std::mutex> lock(queue_mutex);
            if (first != nullptr) {
                node = first;
                first = first->next;
                if (first == nullptr) {
                    last = nullptr;
                }
            }
            is_open = open;
        }
        if (node) {
            out = std::move(node->data);
            delete node;
            success = true;
        }
        return success;
    }

    // close the channel, indicating that either the transmitter(s) or
    // receiver has been destroyed
    // once both ends have been cleaned up, the channel will self-destruct
    // from here
    bool close() {
        const std::lock_guard<std::mutex> lock(queue_mutex);
        bool should_delete = false;
        if (open) {
            open = false;
        } else {
            should_delete = true;
        }
        return should_delete;
    }

    // increment the count of transmitters
    void add_transmitter() {
        const std::lock_guard<std::mutex> lock(tx_count_mutex);
        ++tx_count;
    }

    // decrement the count of transmitters
    // if zero after decrement, close the channel
    bool remove_transmitter() {
        bool should_delete = false;
        {
            const std::lock_guard<std::mutex> lock(tx_count_mutex);
            --tx_count;
            if (tx_count == 0) {
                should_delete = close();
            }
        }
        return should_delete;
    }

    std::mutex queue_mutex;
    std::mutex tx_count_mutex;
    ChannelNode<T> * first;
    ChannelNode<T> * last;
    size_t tx_count;
    bool open;
};

// A Transmitter provides the mechanism for placing data into a channel.
// Transmitters can be created through the open_channel() function,
// moved from another transmitter, or copied. The send() method allows
// users to emplace items at the end of the channel's queue, taking
// ownership of the data in the process.
// Transmitters maintain a shared counter of how many point to the
// same channel, allowing them to only attempt to close it when the
// last transmitter is destroyed.
template<typename T>
class Transmitter {
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    Transmitter() = delete;

    Transmitter(const Transmitter & other) : channel(other.channel) {
        channel->add_transmitter();
    }

    Transmitter& operator=(const Transmitter & other) {
        if (&other == this) return *this;
        close();
        this->channel = other.channel;
        channel->add_transmitter();
        return *this;
    }

    Transmitter(Transmitter && other) noexcept : channel(other.channel) {
        other.channel = nullptr;
    }

    Transmitter& operator=(Transmitter && other) noexcept {
        if (&other == this) return *this;
        close();
        this->channel = other.channel;
        other.channel = nullptr;
        return *this;
    }

    // decrement count of transmitters to channel
    ~Transmitter() {
        close();
    }

    // take ownership of a value and emplace it at the back of the queue
    void send(T & data) {
        channel->push(std::move(data));
    }

    // take ownership of a value and emplace it at the back of the queue
    void send(T && data) {
        channel->push(std::move(data));
    }

    void close() {
        if(channel && channel->remove_transmitter()) {
            delete channel;
        }
        channel = nullptr;
    }

private:
    // point at existing channel. note that the pointer may not be null
    explicit Transmitter(Channel<T> * channel) : channel(channel) {
        channel->add_transmitter();
    }

    Channel<T> * channel;
};

// A Receiver provides the mechanism for popping data off of a Channel's
// FIFO queue. Complementary to the Transmitter, a Receiver relinquishes
// ownership of the received data to the caller, thus leaving the caller
// responsible for managing its lifetime.
// As this type of channel is single-consumer, the Receiver cannot be
// copied or cloned. A Receiver can be constructed via the open_channel()
// method or moved into another Receiver, however.
template<typename T>
class Receiver {
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    Receiver() = delete;

    Receiver(const Receiver &) = delete;

    Receiver& operator=(const Receiver &) = delete;

    // take partial ownership of a Channel from another Receiver
    Receiver(Receiver && other) noexcept : channel(other.channel) {
        other.channel = nullptr;
    }

    // close current Channel, if any,
    // then take partial ownership of the other Receiver's Channel
    Receiver& operator=(Receiver && other) noexcept {
        close();
        this->channel = other.channel;
        other.channel = nullptr;
    }

    // close the current Channel, if any
    ~Receiver() {
        close();
    }

    // attempt to pop an item off the front of the Channel's queue
    // if the queue is empty, returns false with no change to the out parameter
    // otherwise, returns true after moving data into the out parameter
    bool try_receive(T& out, bool& is_open) {
        return channel->try_pop(out, is_open);
    }

    void close() {
        if (channel && channel->close()) {
            delete channel;
        }
        channel = nullptr;
    }

private:
    // constructed following a Channel in the open_channel() method
    // note that the pointer may be null if and only if another
    // Receiver instance is moved into this one before attempting to receive
    explicit Receiver(Channel<T> * channel) : channel(channel) {}

    Channel<T> * channel;
};

// Opens a new Channel for sending data of a specified type between threads.
// Returns a Transmitter and a Receiver that share ownership of the opened
// Channel, and ensure it is cleaned up when both sides are closed and only
// then.
template<typename T>
std::pair<Transmitter<T>, Receiver<T>> open_channel() {
    auto * channel = new Channel<T> {};
    auto tx = Transmitter<T> { channel };
    auto rx = Receiver<T> { channel };
    return std::make_pair(std::move(tx), std::move(rx));
}

#endif //MPSCCHANNEL_CHANNEL_H
