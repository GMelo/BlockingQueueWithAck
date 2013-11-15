package org.gmelo.collection;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * An optionally-bounded thread-safe queue based on linked nodes. This queue orders elements FIFO (first-in-first-out).
 * The head of the queue is that element that has been on the queue the longest time.
 * The tail of the queue is that element that has been on the queue the shortest time.
 * New elements are inserted at the tail of the queue, and the queue retrieval operations
 * obtain elements at the head of the queue.
 * <p/>
 * This queue expects elements to be Acknowledged by the consumer within the time limit otherwise the elements
 * will be pushed back into the Tail of the queue. Negative Acknowledgements will also cause objects to be re-queued.
 * <p/>
 * Null elements are not allowed and will throw exception.
 * <p/>
 * The capacity, if unspecified, is equal to Integer.MAX_VALUE. Linked nodes are dynamically created upon
 * each insertion unless this would bring the queue above capacity.
 * <p/>
 * This Queue is Backed by an link#http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/LinkedBlockingQueue.html
 * <p/>
 * Thread-Safety is enforced by only implementing the safe methods of blocking queues. see http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html
 *
 * @param <T> The type of element to be stored in the queue
 *            <p/>
 *            User: gmelo.org
 */

public class LinkedBlockingQueueWithAck<T> implements BlockingQueueWithAck<T> {

    private final Logger logger = LoggerFactory.getLogger(LinkedBlockingQueueWithAck.class);

    //Storage of elements is deferred to this queue
    private final BlockingQueue<T> internalQueue;
    //Stores elements waiting for acknowledgement
    private final DelayQueue<ExpiryWrapper<T>> waitingForAck = new DelayQueue<ExpiryWrapper<T>>();
    // Timeout before re-queueing objects
    private final long waitBeforeAck;
    //ExecutorService that re-queues expired elements
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    //Map storing the number of times a object was re-queued
    private final Map<T, Integer> countOfElements = new HashMap<T, Integer>();
    //queue that stores objects that were re-queued more than the limit
    private final Queue<T> deadLetterQueue;
    //the maximum number of times a element can be re-queued
    private final int requeueLimit;

    /**
     * Creates a new LinkedBlockingQueueWithAck with a timeout before unacknowledged objects
     * of waitBeforeAck and a capacity of queueSize
     *
     * @param waitBeforeAck   the timeout before elements are re-queued in milliseconds
     * @param queueSize       the maximum capacity of the queue
     * @param maximumRetries  the maximum number of times a element can be requeued before giving up
     * @param deadLetterQueue queue to push the poisoned elements to
     */
    public LinkedBlockingQueueWithAck(long waitBeforeAck, int queueSize, int maximumRetries, Queue<T> deadLetterQueue) {
        this.waitBeforeAck = waitBeforeAck;
        internalQueue = new LinkedBlockingQueue<T>(queueSize);
        requeueLimit = maximumRetries;
        if (deadLetterQueue != null) {
            this.deadLetterQueue = deadLetterQueue;

        } else {
            logger.warn("Poison element queue should not be null");
            this.deadLetterQueue = new LinkedBlockingQueue<T>();
        }
        startExpiryListener();
    }

    /**
     * Creates a new LinkedBlockingQueueWithAck with a timeout before unacknowledged objects
     * of waitBeforeAck and a capacity of Integer.MAX_VALUE
     *
     * @param waitBeforeAck the timeout before elements are re-queued in milliseconds
     */
    public LinkedBlockingQueueWithAck(long waitBeforeAck) {
        this(waitBeforeAck, Integer.MAX_VALUE, Integer.MAX_VALUE, new LinkedBlockingQueue<T>());
    }

    /**
     * Creates a new LinkedBlockingQueueWithAck with a timeout before unacknowledged objects
     * of waitBeforeAck and a capacity of Integer.MAX_VALUE
     *
     * @param waitBeforeAck the timeout before elements are re-queued in milliseconds
     */
    public LinkedBlockingQueueWithAck(long waitBeforeAck, int maximumRequeue) {
        this(waitBeforeAck, Integer.MAX_VALUE, maximumRequeue, new LinkedBlockingQueue<T>());
    }

    /**
     * Creates a new LinkedBlockingQueueWithAck with a timeout before unacknowledged objects
     * of waitBeforeAck, with the provided dead letter queue and a capacity of Integer.MAX_VALUE
     *
     * @param waitBeforeAck the timeout before elements are re-queued in milliseconds
     */
    public LinkedBlockingQueueWithAck(long waitBeforeAck, int maximumRequeue,BlockingQueue<T> deadLetterQ) {
        this(waitBeforeAck, Integer.MAX_VALUE, maximumRequeue, deadLetterQ);
    }

    /**
     * Starts to listen for expired elements on a new thread and re-queues them.
     */
    private void startExpiryListener() {
        executorService.execute(new ExpiryListenerRunner());
    }

    /**
     * Adds element to dead letter queue and cleans up the countOfElements map.
     */
    private void addElementToDeadLetterQueue(T element) {
        logger.debug("adding element {} to dead letter queue");
        deadLetterQueue.add(element);
        countOfElements.remove(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void acknowledge(T element, Acknowledgement acknowledgement) {
        logger.debug("removing object {}", element);

        waitingForAck.remove(new ExpiryWrapper<T>(element, waitBeforeAck));

        if (acknowledgement == Acknowledgement.NACK) {

            if (checkForValidElement(element)) {
                internalQueue.add(element);
            } else {
                addElementToDeadLetterQueue(element);
            }
        }
    }

    /**
     * Exposes a queue with the poisoned elements
     *
     * @return Queue with the elements that were re-queued over the
     */
    public Queue<T> poisonedElements() {
        return deadLetterQueue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int numberOfElementsWaitingForAck() {
        return waitingForAck.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(T t) {
        return internalQueue.add(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(T t) {
        return internalQueue.offer(t);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public T remove() {
        throw new UnsupportedOperationException("Remove operation is not Supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T poll() {
        T element = internalQueue.poll();
        if (element != null) {
            return waitForAck(element);
        }
        return element;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T element() {
        return internalQueue.element();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T peek() {
        return internalQueue.peek();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(T t) throws InterruptedException {
        internalQueue.put(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(T t, long l, TimeUnit timeUnit) throws InterruptedException {
        return internalQueue.offer(t, l, timeUnit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T take() throws InterruptedException {
        T element = internalQueue.take();
        return waitForAck(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T poll(long l, TimeUnit timeUnit) throws InterruptedException {
        T entity = internalQueue.poll(l, timeUnit);
        if (entity != null) {
            return waitForAck(entity);
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int remainingCapacity() {
        return internalQueue.remainingCapacity();
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Remove Operation is not allowed");
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean containsAll(Collection<?> objects) {
        throw new UnsupportedOperationException("Bulk operations are not allowed");
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */

    @Override
    public boolean addAll(Collection<? extends T> ts) {
        throw new UnsupportedOperationException("Add All Operation is not allowed");

    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean removeAll(Collection<?> objects) {
        throw new UnsupportedOperationException("Remove All Operation is not allowed");
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean retainAll(Collection<?> objects) {
        throw new UnsupportedOperationException("Retain All Operation is not allowed");
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("Clear Operation is not allowed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return internalQueue.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return internalQueue.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o) {
        return internalQueue.contains(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<T> iterator() {
        return internalQueue.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        return internalQueue.toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        return internalQueue.toArray(t1s);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public int drainTo(Collection<? super T> objects) {
        throw new UnsupportedOperationException("Drain To Operation is not allowed");
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public int drainTo(Collection<? super T> objects, int i) {
        throw new UnsupportedOperationException("Drain To Operation is not allowed");
    }

    /**
     * Stores an element to the waiting for ack aux queue.
     *
     * @param element the element who will be waiting for the ack.
     * @return the element
     */
    private T waitForAck(T element) {
        waitingForAck.add(new ExpiryWrapper<T>(element, waitBeforeAck));
        return element;

    }

    private boolean checkForValidElement(T element) {
        Integer count = countOfElements.get(element);
        if (count == null) {
            countOfElements.put(element, 2);
            return true;
        }
        if (count >= requeueLimit) {
            return false;
        }
        countOfElements.put(element, count + 1);
        return true;
    }

    /**
     * Delay implementation that wraps a Element of type <T> and gives it a expiry in
     * milliseconds.
     *
     * @param <T>
     */
    private class ExpiryWrapper<T> implements Delayed {

        private final T entity;
        private final long ttl;
        private final long initial;

        public ExpiryWrapper(final T entity, final long ttl) {
            this.entity = entity;
            this.ttl = ttl;
            this.initial = System.currentTimeMillis();
        }

        /**
         * Returns the wrapped element
         *
         * @return the wrpped element T
         */
        public T getEntity() {
            return entity;
        }

        /**
         * {inheritDoc}
         */
        @Override
        public long getDelay(TimeUnit timeUnit) {

            return timeUnit.convert(ttl - (System.currentTimeMillis() - initial), TimeUnit.MILLISECONDS);

        }

        /**
         * {inheritDoc}
         */
        @Override
        public int compareTo(Delayed delayed) {
            long thisTimeLeft = this.getDelay(TimeUnit.MILLISECONDS);
            long thatTimeLeft = delayed.getDelay(TimeUnit.MILLISECONDS);

            return thisTimeLeft > thatTimeLeft ? +1 : thisTimeLeft < thatTimeLeft ? -1 : 0;
        }

        /**
         * /**
         * {inheritDoc}
         * <p/>
         * only takes into account the wrapped element
         *
         * @param o the object to compare to
         * @return true if parameter and entity are the same
         */
        @Override
        @SuppressWarnings("unchecked")
        public boolean equals(Object o) {
            if (o instanceof ExpiryWrapper) {
                ExpiryWrapper that = (ExpiryWrapper) o;

                return entity.equals(that.entity);
            }
            return false;
        }

        /**
         * {inheritDoc}
         * <p/>
         * only takes into account the wrapped entity
         *
         * @return the hashCode of the wrapped entity
         */
        @Override
        public int hashCode() {
            return entity.hashCode();
        }

        @Override
        public String toString() {
            return "ExpiryWrapper{" +
                    "entity=" + entity +
                    '}';
        }
    }

    /**
     * Runnable implementation that blocks while waiting for expired objects in the
     * internal queue and re-queues them.
     */
    private class ExpiryListenerRunner implements Runnable {

        @Override
        public void run() {
            boolean run = true;
            while (run) {
                try {
                    ExpiryWrapper<T> wrappedElement = waitingForAck.take();
                    T element = wrappedElement.getEntity();
                    if (checkForValidElement(element)) {
                        internalQueue.add(element);
                    } else {
                        addElementToDeadLetterQueue(element);
                    }

                    logger.debug("re-queuing object {}", wrappedElement);

                } catch (InterruptedException e) {
                    logger.error("Internal Listener interrupted", e);
                }

            }
        }
    }
}


