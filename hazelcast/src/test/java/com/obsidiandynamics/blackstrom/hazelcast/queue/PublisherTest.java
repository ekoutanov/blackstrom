package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.junit.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.hazelcast.util.executor.*;

public final class PublisherTest extends AbstractPubSubTest {
  private static class TestCallback implements PublishCallback {
    long offset = Record.UNASSIGNED_OFFSET;
    Throwable error;

    @Override
    public void onComplete(long offset, Throwable error) {
      this.offset = offset;
      this.error = error;
    }

    boolean isComplete() {
      return offset != Record.UNASSIGNED_OFFSET || error != null;
    }
  }

  /**
   *  Publishes to a bounded buffer, where the backing store is a NOP.
   *  
   *  @throws InterruptedException
   *  @throws ExecutionException
   */
  @Test
  public void testPublishToBoundedBuffer() throws InterruptedException, ExecutionException {
    final String stream = "s";
    final int capacity = 10;

    final DefaultPublisher p =
        configurePublisher(new PublisherConfig().withStreamConfig(new StreamConfig()
                                                                  .withName(stream)
                                                                  .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = p.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final List<Record> records = new ArrayList<>();
    final List<TestCallback> callbacks = new ArrayList<>();

    final int initialMessages = 5;
    publish(initialMessages, p, records, callbacks);

    assertEquals(initialMessages, records.size());
    assertEquals(initialMessages, callbacks.size());
    await.until(() -> assertEquals(initialMessages, completed(callbacks).size()));
    for (int i = 0; i < initialMessages; i++) {
      assertEquals(i, records.get(i).getOffset());
    }
    assertEquals(initialMessages, buffer.size());
    final List<byte[]> initialItems = readRemaining(buffer, 0);
    assertEquals(initialMessages, initialItems.size());

    final int furtherMessages = 20;
    publish(furtherMessages, p, records, callbacks);

    await.until(() -> assertEquals(initialMessages + furtherMessages, completed(callbacks).size()));
    assertEquals(capacity, buffer.size());
    final List<byte[]> allItems = readRemaining(buffer, 15);
    assertEquals(capacity, allItems.size());
  }
  
  /**
   *  Publishes to a buffer that uses a simple {@link HeapRingbufferStore} as its backing store.
   *  
   *  @throws InterruptedException
   *  @throws ExecutionException
   */
  @Test
  public void testPublishToStoredBuffer() throws InterruptedException, ExecutionException {
    final String stream = "s";
    final int capacity = 10;

    final DefaultPublisher p =
        configurePublisher(new PublisherConfig().withStreamConfig(new StreamConfig()
                                                                  .withName(stream)
                                                                  .withHeapCapacity(capacity)
                                                                  .withStoreFactoryClass(new HeapRingbufferStore.Factory().getClass())));
    final Ringbuffer<byte[]> buffer = p.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final List<Record> records = new ArrayList<>();
    final List<TestCallback> callbacks = new ArrayList<>();

    final int initialMessages = 5;
    publish(initialMessages, p, records, callbacks);

    await.until(() -> assertEquals(initialMessages, completed(callbacks).size()));
    assertEquals(initialMessages, buffer.size());
    final List<byte[]> initialItems = readRemaining(buffer, 0);
    assertEquals(initialMessages, initialItems.size());

    final int furtherMessages = 20;
    publish(furtherMessages, p, records, callbacks);

    await.until(() -> assertEquals(initialMessages + furtherMessages, completed(callbacks).size()));
    assertEquals(capacity, buffer.size());
    final List<byte[]> allItems = readRemaining(buffer, 0);
    assertEquals(initialMessages + furtherMessages, allItems.size());
  }

  /**
   *  Tests publish failure by rigging a mock {@link Ringbuffer} to return a {@link CompletedFuture} with
   *  an error.
   */
  @Test
  public void testPublishFailure() {
    final String stream = "s";
    final int capacity = 10;

    final HazelcastInstance realInstance = newInstance();
    final HazelcastInstance mockInstance = mock(HazelcastInstance.class);
    @SuppressWarnings("unchecked")
    final Ringbuffer<byte[]> mockBuffer = mock(Ringbuffer.class);
    when(mockInstance.<byte[]>getRingbuffer(any())).thenReturn(mockBuffer);
    when(mockInstance.getConfig()).thenReturn(realInstance.getConfig());
    final RuntimeException cause = new RuntimeException("error");
    when(mockBuffer.addAsync(any(), any())).then(invocation -> {
      return new CompletedFuture<>(null, cause, r -> r.run());
    });

    final DefaultPublisher p =
        configurePublisher(mockInstance,
                           new PublisherConfig()
                           .withStreamConfig(new StreamConfig()
                                             .withName(stream)
                                             .withHeapCapacity(capacity)
                                             .withStoreFactoryClass(new HeapRingbufferStore.Factory().getClass())));
    final List<Record> records = new ArrayList<>();
    final List<TestCallback> callbacks = new ArrayList<>();

    publish(1, p, records, callbacks);
    await.until(() -> assertEquals(1, completed(callbacks).size()));
    assertEquals(Record.UNASSIGNED_OFFSET, records.get(0).getOffset());
    assertEquals(Record.UNASSIGNED_OFFSET, callbacks.get(0).offset);
    assertEquals(cause, callbacks.get(0).error);
  }

  private static void publish(int numMessages, Publisher publisher, List<Record> records, List<TestCallback> callbacks) {
    for (int i = 0; i < numMessages; i++) {
      final TestCallback callback = new TestCallback();
      callbacks.add(callback);
      final Record record = new Record("hello".getBytes());
      records.add(record);
      publisher.publishAsync(record, callback);
    }
  }

  private static List<byte[]> readRemaining(Ringbuffer<byte[]> buffer, long startSequence) throws InterruptedException, ExecutionException {
    final ReadResultSet<byte[]> results = buffer
        .readManyAsync(startSequence, 0, 1000, null)
        .get();
    final List<byte[]> items = new ArrayList<>(results.size());
    results.forEach(items::add);
    return items;
  }

  private static List<TestCallback> completed(List<TestCallback> callbacks) {
    return callbacks.stream().filter(c -> c.isComplete()).collect(Collectors.toList());
  }
}