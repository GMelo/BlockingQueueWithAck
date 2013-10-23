package org.gmelo.collection;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
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
 * Thread-Safety is enforced by only implementing the safe methods of blocking queues. see http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html
 *
 * @param <T> The type of element to be stored in the queue
 *
 * User: GMelo
 */
public interface BlockingQueueWithAck<T> extends BlockingQueue<T> {

    public enum Acknowledgement {ACK, NACK}

    /**
     * Acknowledges the reception of a message, either positively or negatively.
     *
     * @param element         the element you are acknowledging
     * @param acknowledgement either ACK or NACK to indicate the status.
     */
    public void acknowledge(T element, Acknowledgement acknowledgement);

    /**
     * Returns the number of unacknowledged elements waiting
     * in the queue.
     *
     * @return the number of elements.
     */
    public int numberOfElementsWaitingForAck();

    /**
     * Returns a queue of elements who are no longer attempting to be sent
     * because they failed to be acknowledged a number of times over the requeue
     * threshold
     *
     * @return a queue containing the "poisoned" elements.
     */
    public Queue<T> poisonedElements();


}
