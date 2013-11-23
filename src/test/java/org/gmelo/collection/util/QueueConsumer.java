package org.gmelo.collection.util;

import org.gmelo.collection.impl.LinkedBlockingQueueWithAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * User: GMelo
 */
public class QueueConsumer<T> implements Consumer<T> {

    protected LinkedBlockingQueueWithAck<T> q;
    private Logger logger = LoggerFactory.getLogger(QueueConsumer.class);

    public QueueConsumer(LinkedBlockingQueueWithAck<T> q) {
        this.q = q;
    }

    public T consume() {

        T element = null;
        try {
            element = q.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Thread interrupted ");

        }
        return element;
    }

}
