package org.gmelo.collection.util;

import org.gmelo.collection.impl.LinkedBlockingQueueWithAck;

/**
 * User: GMelo
 */
public class Consumers<T> {
    private final LinkedBlockingQueueWithAck<T> pushAckQueue;

    public Consumers(LinkedBlockingQueueWithAck<T> pushAckQueueConcurrency) {
        this.pushAckQueue = pushAckQueueConcurrency;
    }

    public Consumer<T> newConsumerWithAck() {
        return new QueueConsumerWithAck<T>(pushAckQueue);
    }

    public Consumer<T> newConsumer() {
        return new QueueConsumer<T>(pushAckQueue);
    }


}
