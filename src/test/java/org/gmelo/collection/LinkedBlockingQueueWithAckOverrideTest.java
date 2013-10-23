package org.gmelo.collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: GMelo
 */
@SuppressWarnings("unchecked")
public class LinkedBlockingQueueWithAckOverrideTest {

    private BlockingQueueWithAck<String> queueWithAck = new LinkedBlockingQueueWithAck<String>(100);
    private String message = "Message";
    private BlockingQueue mockedInternalQueue = Mockito.mock(BlockingQueue.class);
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private long millSeconds = 30L;
    private List<String> stringList = Collections.singletonList(message);


    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException {
        Class clazz = queueWithAck.getClass();
        Field queue = clazz.getDeclaredField("internalQueue");
        queue.setAccessible(true);
        queue.set(queueWithAck, mockedInternalQueue);
    }

    @Test
    public void testAdd() throws Exception {
        queueWithAck.add(message);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).add(message);
    }

    @Test
    public void testOfferNoTimeout() throws Exception {
        queueWithAck.offer(message);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).offer(message);

    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testRemoveHead() throws Exception {
        queueWithAck.remove();
    }

    @Test
    public void testPollNoTimeout() throws Exception {
        queueWithAck.poll();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).poll();

    }

    @Test
    public void testElement() throws Exception {
        queueWithAck.element();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).element();

    }

    @Test
    public void testPeek() throws Exception {
        queueWithAck.peek();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).peek();
    }

    @Test
    public void testPut() throws Exception {
        queueWithAck.put(message);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).put(message);

    }

    @Test
    public void testOfferWTimeout() throws Exception {
        queueWithAck.offer(message, millSeconds, timeUnit);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).offer(message, millSeconds, timeUnit);

    }

    @Test
    public void testTake() throws Exception {
        queueWithAck.take();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).take();

    }

    @Test
    public void testPollWithTimeout() throws Exception {
        queueWithAck.poll(millSeconds, timeUnit);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).poll(millSeconds, timeUnit);

    }

    @Test
    public void testRemainingCapacity() throws Exception {
        queueWithAck.remainingCapacity();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).remainingCapacity();

    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testRemoveObject() throws Exception {
        queueWithAck.remove(message);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testContainsAll() throws Exception {
        queueWithAck.containsAll(stringList);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testAddAll() throws Exception {
        queueWithAck.addAll(stringList);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testRemoveAll() throws Exception {
        queueWithAck.removeAll(stringList);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testRetainAll() throws Exception {
        queueWithAck.retainAll(stringList);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testClear() throws Exception {
        queueWithAck.clear();
    }

    @Test
    public void testSize() throws Exception {
        queueWithAck.size();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).size();
    }

    @Test
    public void testIsEmpty() throws Exception {
        queueWithAck.isEmpty();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).isEmpty();

    }

    @Test
    public void testContains() throws Exception {
        queueWithAck.contains(message);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).contains(message);
    }

    @Test
    public void testIterator() throws Exception {
        queueWithAck.iterator();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).iterator();

    }

    @Test
    public void testToArrayNoParam() throws Exception {
        queueWithAck.toArray();
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).toArray();

    }

    @Test
    public void testToArray() throws Exception {
        queueWithAck.toArray(new String[1]);
        Mockito.verify(mockedInternalQueue, Mockito.atLeastOnce()).toArray(new String[1]);

    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testDrainToParam() throws Exception {
        queueWithAck.drainTo(stringList);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testDrainTo() throws Exception {
        queueWithAck.drainTo(stringList, 0);
    }
}
