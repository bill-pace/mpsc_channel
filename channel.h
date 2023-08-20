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

/**
 * An item within a Channel's queue.
 *
 * A ChannelNode is a single piece of data in a singly linked list.
 * It has no public interface since instances should be managed entirely
 * through the Channel instance that owns them.
 *
 * @tparam T the type of data transferred through the owning Channel
 */
template<typename T>
struct ChannelNode {
    friend Channel<T>;

    ChannelNode() = delete;

    ChannelNode(const ChannelNode &) = delete;

    ChannelNode& operator=(const ChannelNode &) = delete;

    ChannelNode(ChannelNode &&) = delete;

    ChannelNode& operator=(ChannelNode &&) = delete;

private:
    /**
     * Construct a ChannelNode.
     *
     * Takes ownership of the `data` parameter.
     *
     * @param data An instance of the template type.
     */
    explicit ChannelNode(T && data) : data(data), next(nullptr) {}

    T data;
    ChannelNode* next;
};

/**
 * A FIFO queue for transferring data between threads.
 *
 * A Channel enables the transfer of data from one or more threads
 * to another through the use of the Transmitter(s) and Receiver that
 * connect to the Channel. The Receiver will remove items from the
 * Channel in the same order that Transmitters add them, though if
 * multiple Transmitters point to the same Channel there will be a
 * (benign) race condition on which one adds its item first.
 *
 * Thread safety is achieved through locking either of two mutexes,
 * depending on the operation being performed. One mutex protects
 * the queue and is locked for pushing and popping; the other
 * protects the count of live Transmitters and is locked when they
 * are created, destroyed, copied, or moved.
 *
 * Channels take ownership of items pushed into them and relinquish
 * ownership of items popped out, and so cleanup should generally
 * happen on the side of the Receiver. If a Channel is nonempty
 * when its destructor is called, it will clean up all of its
 * ChannelNodes and, therefore, the data that was transmitted but
 * never received.
 *
 * Channels have no public interface as they should be entirely
 * managed through the Transmitter(s) and Receiver that point to
 * them. Once one end of the Channel has been closed (e.g. by
 * moving or destroying all Transmitter instances that point to it)
 * the Channel will be marked as closed, and when the other end
 * is closed the Channel's destructor will be called.
 *
 * @tparam T the type of data transferred through the Channel
 */
template<typename T>
class Channel {
    friend Transmitter<T>;
    friend Receiver<T>;
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    Channel(const Channel&) = delete;

    Channel& operator=(const Channel&) = delete;

    Channel(Channel&&) = delete;

    Channel& operator=(Channel&&) = delete;

private:
    /**
     * Create a new Channel.
     *
     * By default, a Channel is open, empty, and unlocked. This
     * constructor should only be called from the `open_channel<T>()`
     * function, which will assign a Transmitter and a Receiver to
     * the created Channel and pass those back to the caller.
     */
    Channel() :
            queue_mutex(),
            tx_count_mutex(),
            first(nullptr),
            last(nullptr),
            tx_count(0),
            open(true)
    {}

    /**
     * Destroy a Channel and its owned ChannelNodes.
     *
     * Only call this method from a Transmitter or Receiver
     * that is being closed, after it checks whether the
     * other end of the Channel has already been closed.
     */
    ~Channel() {
        ChannelNode<T> * next = first;
        while (next) {
            ChannelNode<T> * following = next->next;
            delete next;
            next = following;
        }
    }

    /**
     * Add an item to the queue.
     *
     * Prepare an item of the Channel's template type to be moved
     * across threads. Take ownership of the item to ensure that
     * other threads cannot corrupt the data before the Receiver
     * claims it.
     *
     * Locks the `queue_mutex` after constructing a new ChannelNode
     * to ensure that the queue remains in a valid state without
     * forcing other threads to wait for heap allocation.
     *
     * Pushing to a closed queue effectively throws the data away,
     * as it will be moved into a ChannelNode instance that can
     * never be retrieved.
     *
     * Note that the queue's capacity is determined by the heap
     * memory available, and so pushing can only fail based on
     * queue length if memory runs out.
     *
     * @param new_value The data to enqueue
     */
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

    /**
     * Remove an item from the queue.
     *
     * Locks the `queue_mutex` to check whether the queue is currently empty and
     * whether it is currently open. If the queue is empty, `try_pop` will return
     * false and make no change to the output parameter `out`. If the queue is
     * not empty, `try_pop` will remove the first ChannelNode, move its data into
     * the output parameter `out`, and return true. In both cases, the output
     * parameter `is_open` will be set to the current value of the Channel's
     * member data `open`.
     *
     * The `queue_mutex` remains locked only long enough to modify the queue and
     * check the value of the `open` data member, ensuring the queue remains in
     * a valid state without forcing other threads to wait on move assignment or
     * heap deallocation.
     *
     * @param out Output parameter to hold data moved out of the queue
     * @param is_open Output parameter indicating whether the Channel is still open
     * @return whether the parameter `out` was modified
     */
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

    /**
     * Direct the Channel to close.
     *
     * Locks the `queue_mutex` to check whether the Channel is currently
     * open. If so, marks the Channel as closed by setting `open` to false,
     * then returns false to indicate that one half of the Channel still
     * exists and the caller should not invalidate it by deleting the
     * Channel.
     *
     * Otherwise, returns true to indicate to the caller that it is the
     * last owner of the Channel and that the Channel should now be
     * deleted to ensure it isn't leaked.
     *
     * @return whether the caller should invoke the Channel's destructor
     */
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

    /**
     * Increments the Transmitter count.
     *
     * Locks the `tx_count_mutex` and then increments `tx_count`
     * to track the assignment of another Transmitter to this
     * Channel.
     */
    // increment the count of transmitters
    void add_transmitter() {
        const std::lock_guard<std::mutex> lock(tx_count_mutex);
        ++tx_count;
    }

    /**
     * Decrements the Transmitter count.
     *
     * Locks the `tx_count_mutex` and then decrements `tx_count`
     * to track the removal of a Transmitter from this Channel.
     *
     * If the count reaches zero, then the caller is the last
     * Transmitter to this Channel and no more can be created.
     * The Channel then calls its own `close` method and passes
     * the result to its caller to indicate whether the
     * destructor should also be invoked.
     *
     * If the count remains above zero, then other Transmitters
     * to this Channel are still live and it should be neither
     * closed nor destroyed.
     *
     * @return whether the destructor should be invoked next
     */
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
