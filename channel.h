#ifndef MPSCCHANNEL_CHANNEL_H
#define MPSCCHANNEL_CHANNEL_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <utility>

template<typename T> class Receiver;
template<typename T> class Transmitter;
template<typename T> std::pair<Transmitter<T>, Receiver<T>> open_channel();

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
 * Thread safety is achieved through either locking a mutex for
 * pushing to/popping from the queue, or atomically tracking the
 * number of live Transmitters.@n@n
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

    /**
     * @brief Container for a single item
     *
     * This private struct serves as a node within the Channel's
     * underlying singly linked list.
     */
    struct ChannelNode {
        T data;
        ChannelNode * next;
        explicit ChannelNode(T && data) : data(std::move(data)), next(nullptr) {};
    };

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
            cv(),
            queue_mutex(),
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
        std::lock_guard<std::mutex> lock(queue_mutex);
        ChannelNode * next = first;
        while (next) {
            ChannelNode * following = next->next;
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
        auto * new_node = new ChannelNode { std::move(new_value) };
        const std::lock_guard<std::mutex> lock(queue_mutex);
        if (last) {
            last->next = new_node;
        } else {
            first = new_node;
        }
        last = new_node;
        cv.notify_one();
    }

    /**
     * @brief Attempt to move an item from the queue.
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
        ChannelNode * node = nullptr;
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
     * @brief Attempt to wait for an item on the queue.
     *
     * Locks the `queue_mutex` to check whether the queue is currently empty
     * and open. If so, waits for notification via the `cv` condition variable
     * that either the queue is nonempty or the Channel is closed. Once that
     * condition is satisfied, attempts to remove an item from the queue as in
     * `try_pop()`.
     *
     * @param out Output parameter to hold data moved out of the queue
     * @param is_open Output parameter indicating whether the Channel is still open
     * @return whether the parameter `out` was modified
     */
    bool wait_pop(T& out, bool& is_open) {
        bool success = false;
        ChannelNode * node = nullptr;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock, [&]{ return first != nullptr || !open; });
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
     * Atomically increments `tx_count` to track the assignment
     * of another Transmitter to this Channel.
     */
    void add_transmitter() {
        // safe to use relaxed operations here because the queue_mutex
        // will handle acquire/release ordering on the other data
        tx_count.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * @brief Decrements the Transmitter count.
     *
     * Atomically decrements `tx_count` to track the removal
     * of a Transmitter from this Channel.@n@n
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
        // safe to use relaxed operations here because the queue_mutex will handle acquire/release ordering on the
        // other data
        unsigned transmitters_before_decrement = tx_count.fetch_sub(1, std::memory_order_relaxed);
        // if 1 transmitter was left before the decrement, then we have just removed the last transmitter that
        // could have sent any messages to this channel and should close it down
        if (transmitters_before_decrement == 1) {
            std::lock_guard<std::mutex> lock(queue_mutex);
            should_delete = close();
            if (!should_delete) {
                // Receiver is still live, so notify all threads waiting on it to either claim a value or stop
                // waiting for something that can no longer be sent
                cv.notify_all();
            }
        }
        return should_delete;
    }

    bool remove_receiver() {
        bool should_delete = false;
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            should_delete = close();
        }
        return should_delete;
    }

    std::condition_variable cv;
    std::mutex queue_mutex;
    ChannelNode * first;
    ChannelNode * last;
    std::atomic_uint tx_count;
    bool open;
};

/**
 * @brief A class for emplacing items in a Channel.
 *
 * Transmitters provide the mechanism to add items to a Channel's queue.
 * As this is a multi-producer Channel, Transmitters may be either
 * copied or moved. A new Transmitter may be default-constructed with
 * no target Channel, but the only way to create a new Transmitter that
 * points at a live Channel is with the `open_channel<T>()` factory
 * function.@n@n
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
    /**
     * Create a Transmitter with no target Channel.
     */
    Transmitter() : channel(nullptr) {};

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
     * of active Transmitters wouldn't need to change.
     *
     * @param other The Transmitter to copy
     * @return a reference to this Transmitter
     */
    Transmitter& operator=(const Transmitter & other) {
        if (&other == this) return *this;
        if (other.channel == this->channel) return *this;
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
     * does not update the counter.
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
     * Transmitter. Updates the Transmitter count on its
     * original Channel, but not on the new one as the count
     * of live Transmitters to the new one doesn't need to
     * change.
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

    /**
     * @brief Checks if the Transmitter is still open
     *
     * @return whether the Transmitter still has a target Channel
     */
    bool is_valid() {
        return channel != nullptr;
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

/**
 * @brief A class for removing items from a Channel.
 *
 * Receivers provide the mechanism for removing items from a Channel's
 * queue. As this is a single-consumer Channel, Receivers may be moved
 * or created through the `open_channel<T>()` factory function, but
 * they may not be copied. Receivers may also be default-constructed
 * with no target Channel.@n@n
 *
 * When moved, destroyed, or directed to close, a Receiver will first
 * close its target Channel, or destroy the Channel if it has already
 * been closed from the Transmitter end.@n@n
 *
 * The `try_receive()` method checks whether the target Channel has
 * anything in its queue, retrieving the first item if so, and will
 * also indicate whether the Channel is still open. The caller may
 * choose to use this status as an indicator of when to stop trying
 * to do work with a Receiver.
 *
 * @tparam T the type of item that can be received
 */
template<typename T>
class Receiver {
    friend std::pair<Transmitter<T>, Receiver<T>> open_channel<T>();

public:
    /**
     * Create a Receiver with no target Channel.
     */
    Receiver() : channel(nullptr) {};

    Receiver(const Receiver &) = delete;

    Receiver& operator=(const Receiver &) = delete;

    /**
     * @brief Move construct a Receiver.
     *
     * Takes partial ownership of the `other` Receiver's target
     * Channel instance and then closes the `other` Receiver.
     *
     * @param other The Receiver to move from
     */
    Receiver(Receiver && other) noexcept : channel(other.channel) {
        other.channel = nullptr;
    }

    /**
     * @brief Move assign a Receiver.
     *
     * Instructs the currently targeted Channel to close, or
     * deletes it if it is already closed, then moves from
     * the `other` Receiver.
     *
     * @param other The Receiver to move from
     * @return this
     */
    Receiver& operator=(Receiver && other) noexcept {
        close();
        this->channel = other.channel;
        other.channel = nullptr;
        return *this;
    }

    /**
     * @brief Destroy a Receiver.
     *
     * First instructs the target Channel, if any, to close,
     * and if it is already closed, deletes it.
     */
    ~Receiver() {
        close();
    }

    /**
     * @brief Attempt to retrieve an item from the target Channel.
     *
     * Attempts to move data from the target Channel into the provided
     * `out` parameter. The return value indicates whether `out` was
     * actually modified, as `try_receive()` cannot retrieve an item
     * from an empty queue. This method also indicates whether the
     * Channel still has at least one active Transmitter, as an
     * empty Channel with no Transmitters will never contain items
     * again and may indicate that the caller should stop trying
     * to retrieve items from this Receiver.@n@n
     *
     * Will segfault if the Receiver has been closed, i.e. by moving
     * out of it or by directly invoking its `close()` method.
     *
     * @param out Output parameter to contain the popped item
     * @param is_open Output parameter indicating whether the target Channel is open
     * @return whether `out` was modified
     */
    bool try_receive(T& out, bool& is_open) {
        return channel->try_pop(out, is_open);
    }

    /**
     * @brief Attempt to wait for an item in the target Channel.
     *
     * Checks the target Channel for an item and waits for it to gain one
     * if the Channel is empty. Will move data from the Channel into the
     * `out` parameter, record whether the Channel is still open in the
     * `is_open` parameter, and return true when the Channel gains an item.
     * Alternatively, if the Channel is closed before anything else is
     * transmitted to it, will return false as the Receiver stops waiting.@n@n
     *
     * Will segfault if the Receiver has been closed, i.e. by moving
     * out of it or by directly invoking its `close()` method.
     *
     * @param out Output parameter to contain the popped item
     * @param is_open Output parameter indicating whether the target Channel is open
     * @return whether `out` was modified
     */
    bool wait_receive(T& out, bool& is_open) {
        return channel->wait_pop(out, is_open);
    }

    /**
     * @brief Disconnect from the target Channel
     *
     * Instructs the target Channel, if any, to close. If it is
     * already closed, deletes the Channel.
     */
    void close() {
        if (channel && channel->remove_receiver()) {
            delete channel;
        }
        channel = nullptr;
    }

    /**
     * @brief Checks if the Receiver is still open
     *
     * @return whether the Receiver still has a target Channel
     */
     bool is_valid() {
         return channel != nullptr;
     }

private:
    /**
     * @brief Construct a new Receiver for a Channel
     *
     * Creates a new Receiver that points to the specified Channel.
     * Should only be called from the `open_channel<T>()` factory
     * function, which will supply a non-null pointer.
     *
     * @param channel Pointer to the target Channel
     */
    explicit Receiver(Channel<T> * channel) : channel(channel) {}

    Channel<T> * channel;
};

/**
 * @brief Opens a new Channel.
 *
 * Creates a new Channel on the heap for sending data across threads, as
 * well as a Transmitter and a Receiver that both target the Channel.
 * The Transmitter and Receiver share ownership of the Channel and will
 * ensure it is properly cleaned up once both sides have been destroyed.
 *
 * @tparam T type of data to be transmitted through the opened Channel; must support move semantics
 * @return a `std::pair` of Transmitter and Receiver instances that target the opened Channel
 */
template<typename T>
std::pair<Transmitter<T>, Receiver<T>> open_channel() {
    auto * channel = new Channel<T> {};
    auto tx = Transmitter<T> { channel };
    auto rx = Receiver<T> { channel };
    return std::make_pair(std::move(tx), std::move(rx));
}

#endif //MPSCCHANNEL_CHANNEL_H
