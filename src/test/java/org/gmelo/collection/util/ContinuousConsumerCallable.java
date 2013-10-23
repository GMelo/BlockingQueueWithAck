package org.gmelo.collection.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * User: GMelo
 */
public class ContinuousConsumerCallable<T> implements Callable<List<T>> {

    private Consumer<T> consumer;

    private final Logger logger = LoggerFactory.getLogger(ContinuousConsumerCallable.class);

    public ContinuousConsumerCallable(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public List<T> call() throws Exception {
        List<T> list = new ArrayList<T>();
        T e;
        do {
            e = consumer.consume();
            if (e != null)
                list.add(e);

        } while (e != null);

        logger.info("Left Loop");
        return list;
    }
}
