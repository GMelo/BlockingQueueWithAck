package org.gmelo.collection.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * User: GMelo
 */
public class Producer implements Callable<String> {

    private final Queue q;
    private final int name;
    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    public Producer(int n, Queue q) {
        this.q = q;
        this.name = n;
    }


    @Override
    public String call() throws Exception {
        for (int i = 0; i < 100; i++) {
            String string = "String number " + i + "From producer " + name;
            q.add(string);
            logger.debug(string);
        }
        return "OK";
    }
}
