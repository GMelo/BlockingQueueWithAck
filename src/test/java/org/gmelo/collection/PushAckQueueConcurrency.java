package org.gmelo.collection;

import junit.framework.Assert;
import org.gmelo.collection.impl.LinkedBlockingQueueWithAck;
import org.gmelo.collection.util.Consumers;
import org.gmelo.collection.util.ContinuousConsumerCallable;
import org.gmelo.collection.util.Producer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * User: GMelo
 */
public class PushAckQueueConcurrency {

    private final Logger logger = LoggerFactory.getLogger(PushAckQueueConcurrency.class);

    @Test
    public void testMassiveConcurrencyWithResend() throws InterruptedException, ExecutionException {
        logger.info("Starting Test");
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(200);
        ExecutorService producers = Executors.newFixedThreadPool(10);

        Collection<Callable<String>> producerList = new ArrayList<Callable<String>>();

        for (int i = 0; i < 10; i++) {
            producerList.add(new Producer(i, queue));
        }

        ExecutorService consumers = Executors.newFixedThreadPool(25);

        Consumers<String> consumerFactory = new Consumers<String>(queue);
        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();

        for (int i = 0; i < 10; i++) {
            ContinuousConsumerCallable<String> reliable = new ContinuousConsumerCallable<String>(consumerFactory.newConsumerWithAck());
            ContinuousConsumerCallable<String> unreliable = new ContinuousConsumerCallable<String>(consumerFactory.newConsumer());

            futures.add(consumers.submit(reliable));
            futures.add(consumers.submit(unreliable));

        }

        producers.invokeAll(producerList);

        producers.shutdown();
        consumers.shutdown();
        consumers.awaitTermination(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, queue.numberOfElementsWaitingForAck());
        Assert.assertEquals(0, queue.size());

        //removing duplicate messages
        Set<String> strings = new HashSet<String>();
        for (Future<List<String>> future : futures) {
            strings.addAll(future.get());
        }
        Assert.assertEquals(1000, strings.size());
    }

    @Test
    public void testMassiveConcurrency() throws InterruptedException, ExecutionException {
        logger.info("Starting Test");
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(1000);
        ExecutorService producers = Executors.newFixedThreadPool(10);

        Collection<Callable<String>> producerList = new ArrayList<Callable<String>>();

        for (int i = 0; i < 10; i++) {
            producerList.add(new Producer(i, queue));
        }

        ExecutorService consumers = Executors.newFixedThreadPool(25);

        Consumers<String> consumerFactory = new Consumers<String>(queue);
        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();

        for (int i = 0; i < 10; i++) {
            ContinuousConsumerCallable<String> reliable = new ContinuousConsumerCallable<String>(consumerFactory.newConsumerWithAck());

            futures.add(consumers.submit(reliable));

        }

        producers.invokeAll(producerList);

        producers.shutdown();
        consumers.shutdown();
        consumers.awaitTermination(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, queue.numberOfElementsWaitingForAck());
        Assert.assertEquals(0, queue.size());

        List<String> strings = new ArrayList<String>();
        for (Future<List<String>> future : futures) {
            strings.addAll(future.get());
        }
        Assert.assertEquals(1000, strings.size());
    }
}
