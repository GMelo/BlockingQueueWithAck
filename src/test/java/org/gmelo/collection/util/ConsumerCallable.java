package org.gmelo.collection.util;

import java.util.concurrent.Callable;

/**
 * User: GMelo
 */
public class ConsumerCallable<T> implements Callable<T> {

    private Consumer<T> consumer;

    public ConsumerCallable(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public T call() throws Exception {
        return consumer.consume();
    }
}
