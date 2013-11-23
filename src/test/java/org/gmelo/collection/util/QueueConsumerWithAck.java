package org.gmelo.collection.util;

import org.gmelo.collection.impl.LinkedBlockingQueueWithAck;

/**
 * User: GMelo
 */

public class QueueConsumerWithAck<T> extends QueueConsumer<T> {

    public QueueConsumerWithAck(LinkedBlockingQueueWithAck<T> q) {
        super(q);
    }

    @Override
    public T consume() {
        T element = super.consume();
        if (element != null)
            q.acknowledge(element, LinkedBlockingQueueWithAck.Acknowledgement.ACK);
        return element;
    }
}
