package org.gmelo.collection;

import junit.framework.Assert;
import org.gmelo.collection.impl.LinkedBlockingQueueWithAck;
import org.gmelo.collection.util.ConsumerCallable;
import org.gmelo.collection.util.Consumers;
import org.junit.Test;

import java.util.concurrent.*;

/**
 * User: GMelo
 */

public class LinkedBlockingQueueWithAckTest {

    @Test
    public void testReceiveResend() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(100);
        Consumers<String> consumers = new Consumers<String>(queue);

        String message = "Some Message";
        queue.add(message);

        Callable<String> consumeWithAck = new ConsumerCallable<String>(consumers.newConsumerWithAck());

        Callable<String> consumeWithoutAck = new ConsumerCallable<String>(consumers.newConsumer());

        ExecutorService service = Executors.newFixedThreadPool(2);
        Future f = service.submit(consumeWithoutAck);
        Assert.assertEquals(message, f.get());

        f = service.submit(consumeWithAck);

        Assert.assertEquals(message, f.get());

        Assert.assertTrue(queue.isEmpty());
        service.shutdown();

    }

    @Test
    public void testReceiveSentBeforeRegister() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(100);
        Consumers<String> consumers = new Consumers<String>(queue);
        String message = "Some Message";
        queue.add(message);

        ExecutorService service = Executors.newSingleThreadExecutor();

        Future f = service.submit(new ConsumerCallable<String>(consumers.newConsumerWithAck()));


        Assert.assertEquals(message, f.get());

        service.shutdown();

    }

    @Test
    public void testReceiveSentAfterRegister() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(100);
        Consumers<String> consumers = new Consumers<String>(queue);
        String message = "Some Message";


        ExecutorService service = Executors.newSingleThreadExecutor();

        Future f = service.submit(new ConsumerCallable<String>(consumers.newConsumerWithAck()));


        queue.add(message);

        Assert.assertEquals(message, f.get());

        service.shutdown();

    }

    @Test
    public void testAck() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(50);

        queue.add("Something");

        String s = queue.take();

        queue.acknowledge(s, LinkedBlockingQueueWithAck.Acknowledgement.ACK);

        Thread.sleep(50);

        Assert.assertEquals(0, queue.size());

    }

    @Test
    public void testNAck() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(50);

        queue.add("Something");

        String s = queue.take();
        Thread.sleep(55);
        queue.acknowledge(s, LinkedBlockingQueueWithAck.Acknowledgement.NACK);

        Assert.assertEquals(1, queue.size());

    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testNull() throws ExecutionException, InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(50);

        queue.add(null);

    }

    @Test
    public void testPoisonAckQueue() throws InterruptedException {
        LinkedBlockingQueueWithAck<String> queue = new LinkedBlockingQueueWithAck<String>(10, 3);

        String message = "Message";
        queue.add(message);
        queue.take();
        queue.take();
        queue.take();
        queue.take();
        String s = queue.poll(15, TimeUnit.MILLISECONDS);

        Assert.assertEquals(message, queue.poisonedElements().peek());
        Assert.assertNull(s);


    }


}
