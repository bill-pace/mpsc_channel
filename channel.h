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
 * @brief An item within a Channel's queue.
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
     * @brief Construct a ChannelNode.
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
 * @brief A FIFO queue for transferring data between threads.
 *
 * A Channel enables the transfer of data from one or more threads
 * to another through the use of the Transmitter(s) and Receiver that
 * connect to the Channel. The Receiver will remove items from the
 * Channel in the same order that Transmitters add them, though if
 * multiple Transmitters point to the same Channel there will be a
 * (benign) race condition on which one adds its item first.@n@n
 *
 * Thread safety is achieved through locking either of two mutexes,
 * depending on the operation being performed. One mutex protects
 * the queue and is locked for pushing and popping; the other
 * protects the count of live Transmitters and is locked when they
 * are created, destroyed, copied, or moved.@n@n
 *
 * Channels take ownership of items pushed into them and relinquish
 * ownership of items popped out, and so cleanup should generally
 * happen on the side of the Receiver. If a Channel is nonempty
 * when its destructor is called, it will clean up all of its
 * ChannelNodes and, therefore, the data that was transmitted but
 * never received.@n@n
 *
 * Channels have no public interface as they should be entirely
 * managed through the Transmitter(s) and Receiver that point to
 * them. Once one end of the Channel has been closed (e.g. by
 * moving or destroying all Transmitter instances that point to it)
 * the Channel will be marked as closed, and when the other end
 * is closed the Channel's destructor will be called.@n@n
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
     * @brief Create a new Channel.
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
     * @brief Destroy a Channel and its owned ChannelNodes.
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
     * @brief Add an item to the queue.
     *
     * Prepare an item of the Channel's template type to be moved
     * across threads. Take ownership of the item to ensure that
     * other threads cannot corrupt the data before the Receiver
     * claims it.@n@n
     *
     * Locks the `queue_mutex` after constructing a new ChannelNode
     * to ensure that the queue remains in a valid state without
     * forcing other threads to wait for heap allocation.@n@n
     *
     * Pushing to a closed queue effectively throws the data away,
     * as it will be moved into a ChannelNode instance that can
     * never be retrieved.@n@n
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
     * @brief Remove an item from the queue.
     *
     * Locks the `queue_mutex` to check whether the queue is currently empty and
     * whether it is currently open. If the queue is empty, `try_pop` will return
     * false and make no change to the output parameter `out`. If the queue is
     * not empty, `try_pop` will remove the first ChannelNode, move its data into
     * the output parameter `out`, and return true. In both cases, the output
     * parameter `is_open` will be set to the current value of the Channel's
     * member data `open`.@n@n
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
     * @brief Direct the Channel to close.
     *
     * Locks the `queue_mutex` to check whether the Channel is currently
     * open. If so, marks the Channel as closed by setting `open` to false,
     * then returns false to indicate that one half of the Channel still
     * exists and the caller should not invalidate it by deleting the
     * Channel.@n@n
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
     * @brief Increments the Transmitter count.
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
     * @brief Decrements the Transmitter count.
     *
     * Locks the `tx_count_mutex` and then decrements `tx_count`
     * to track the removal of a Transmitter from this Channel.
     * @n@n
     *
     * If the count reaches zero, then the caller is the last
     * Transmitter to this Channel and no more can be created.
     * The Channel then calls its own `close` method and passes
     * the result to its caller to indicate whether the
     * destructor should also be invoked.@n@n
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

/**
 * @brief A class for emplacing items in a Channel.
 *
 * Transmitters provide the mechanism to add items to a Channel's queue.
 * As this is a multi-producer Channel, Transmitters may be either
 * copied or moved, though they may only be constructed through the
 * `open_channel<T>()` factory function.@n@n
 *
 * When copied, moved, destroyed, or directed to close, a Transmitter
 * will notify the corresponding Channel instance(s) for the sake of
 * updating the count of Transmitters to each Channel. When breaking
 * a connection, if the invoked Transmitter is the last one connected
 * to its Channel, it will either close the Channel or delete it,
 * depending on whether the Receiver is still alive.@n@n
 *
 * The `send()` method moves the provided data into the Transmitter's
 * connected Channel, and once it returns the caller can continue
 * operation without concern for the Channel's state.
 *
 * @tparam T the type of data transmitted
 */
template<typename T>
class Transmitter {
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    Transmitter() = delete;

    /**
     * @brief Copy construct a Transmitter.
     *
     * Copy the `channel` pointer from the copied Transmitter
     * and invoke `add_trasmitter()` on the referent, if not
     * null.
     *
     * @param other The Transmitter to copy
     */
    Transmitter(const Transmitter & other) : channel(other.channel) {
        if (channel) channel->add_transmitter();
    }

    /**
     * @brief Copy assign a Transmitter.
     *
     * Copy the `channel` pointer from the copied Transmitter
     * and invoke `add_transmitter()` on the referent, if not
     * null. Does no work if the `channel` pointers are equal
     * between the two Transmitter instances, as the count
     * of active Transmitters wouldn't need to change in that
     * case so there's no point in waiting for the mutex lock.
     *
     * @param other The Transmitter to copy
     * @return a reference to this Transmitter
     */
    Transmitter& operator=(const Transmitter & other) {
        if (&other == this) return *this;
        if (other.channel == this->channel) return *this; // Transmitter count won't change so don't wait for the mutex
        close();
        this->channel = other.channel;
        if(channel) channel->add_transmitter();
        return *this;
    }

    /**
     * @brief Move construct a Transmitter.
     *
     * Copies the `channel` pointer from the moved Transmitter
     * before setting it to null on the moved Transmitter. As
     * moving a Transmitter doesn't change the count of valid
     * Transmitters to the target Channel, this constructor
     * does not wait for the mutex lock.
     *
     * @param other The Transmitter to move from
     */
    Transmitter(Transmitter && other) noexcept : channel(other.channel) {
        other.channel = nullptr;
    }

    /**
     * @brief Move assign a Transmitter.
     *
     * Disconnects this Transmitter from its Channel, if any,
     * then moves the `channel` pointer from the `other`
     * Transmitter. Waits for the mutex lock on its original
     * Channel, but not on the new one as the count of live
     * Transmitters to the new one doesn't need to change.
     *
     * @param other
     * @return
     */
    Transmitter& operator=(Transmitter && other) noexcept {
        if (&other == this) return *this;
        close();
        this->channel = other.channel;
        other.channel = nullptr;
        return *this;
    }

    /**
     * @brief Destroy a Transmitter instance.
     *
     * Invokes the `close()` method to check whether the
     * pointed-to Channel should also be destroyed.
     */
    ~Transmitter() {
        close();
    }

    /**
     * @brief Transmit a value to the target Channel.
     *
     * Moves data out of the parameter and into the target
     * Channel via the Channel's `push()` method. This
     * method will segfault if the Transmitter it was
     * invoked on has been closed, i.e. by moving out of
     * it or by directly invoking its `close()` method.
     *
     * @param item The value to transmit
     */
    void send(T & item) {
        channel->push(std::move(item));
    }

    /**
     * @brief Transmit a value to the target Channel.
     *
     * Moves data out of the parameter and into the target
     * Channel via the Channel's `push()` method. This
     * method will segfault if the Transmitter it was
     * invoked on has been closed, i.e. by moving out of
     * it or by directly invoking its `close()` method.
     *
     * @param item The value to transmit
     */
    void send(T && item) {
        channel->push(std::move(item));
    }

    /**
     * @brief Disconnect from the target Channel.
     *
     * Checks whether the target Channel, if not null, has
     * other Transmitters connected to it. If so, sets the
     * `channel` pointer to null and returns. If not, and
     * the Channel reports that it has already been closed
     * from the Receiver end, deletes the Channel.
     */
    void close() {
        if(channel && channel->remove_transmitter()) {
            delete channel;
        }
        channel = nullptr;
    }

private:
    /**
     * @brief Construct a new Transmitter.
     *
     * Creates a new Transmitter instance that targets the
     * specified Channel. Should only be called from the
     * `open_channel<T>()` factory function. Note that the
     * `channel` parameter should be non-null to ensure
     * that the Transmitter begins in a usable state.
     *
     * @param channel Pointer to the target Channel
     */
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
