package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class SubscriberFreeTest extends AbstractPubSubTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  /**
   *  Deactivation can only be performed in a group-aware context.
   */
  @Test(expected=IllegalStateException.class)
  public void testIllegalDeactivate() {
    final String stream = "s";
    final int capacity = 1;
    
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                    .withName(stream)
                                                                    .withHeapCapacity(capacity)));
    s.deactivate();
  }
  
  /**
   *  Reactivation can only be performed in a group-aware context.
   */
  @Test(expected=IllegalStateException.class)
  public void testIllegalReactivate() {
    final String stream = "s";
    final int capacity = 1;
    
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                    .withName(stream)
                                                                    .withHeapCapacity(capacity)));
    s.reactivate();
  }
  
  /**
   *  Offset confirmation can only be performed in a group-aware context.
   */
  @Test(expected=IllegalStateException.class)
  public void testIllegalConfirm() {
    final String stream = "s";
    final int capacity = 1;
    
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                    .withName(stream)
                                                                    .withHeapCapacity(capacity)));
    s.confirm();
  }

  /**
   *  Tests consuming from an empty buffer. Should result in a zero-size batch.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConsumeEmpty() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    assertNotNull(s.getConfig());
    assertTrue(s.isAssigned()); 
    
    final RecordBatch b0 = s.poll(1);
    assertEquals(0, b0.size());
    
    final RecordBatch b1 = s.poll(1);
    assertEquals(0, b1.size());
    
    verifyNoError(eh);
  }

  /**
   *  Simple scenario of consuming a single message.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConsumeOne() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));

    buffer.add("hello".getBytes());

    final RecordBatch b0 = s.poll(1_000);
    assertEquals(1, b0.size());
    assertArrayEquals("hello".getBytes(), b0.all().get(0).getData());

    final RecordBatch b1 = s.poll(10);
    assertEquals(0, b1.size());
    
    verifyNoError(eh);
  }

  /**
   *  Consumes two messages.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConsumeTwo() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));

    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    final RecordBatch b0 = s.poll(1_000);
    assertEquals(2, b0.size());
    assertArrayEquals("h0".getBytes(), b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), b0.all().get(1).getData());
    
    final RecordBatch b1 = s.poll(10);
    assertEquals(0, b1.size());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests the consumption of two messages and then a seek back by one position, so that the last
   *  message can be consumed again.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testSeek() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    s.seek(1);
    final RecordBatch b0 = s.poll(1_000);
    assertEquals(1, b0.size());
    assertArrayEquals("h1".getBytes(), b0.all().get(0).getData());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests a seek to an illegal position.
   *  
   *  @throws InterruptedException
   */
  @Test(expected=IllegalArgumentException.class)
  public void testSeekIllegalArgumentTooLow() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                    .withName(stream)
                                                                    .withHeapCapacity(capacity)));
    s.seek(-5);
  }
  
  /**
   *  Tests read failure. This is achieved by simulating a buffer overflow with no backing
   *  storage, so that the subscriber consumes from a stale sequence.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testReadFailure() throws InterruptedException {
    final String stream = "s";
    final int capacity = 1;
    final ErrorHandler errorHandler = mock(ErrorHandler.class);

    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(errorHandler)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)
                                              .withStoreFactoryClass(null)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));

    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    final RecordBatch b = s.poll(1_000);
    assertEquals(0, b.size());
    verify(errorHandler).onError(isNotNull(), (Exception) isNotNull());
  }

  /**
   *  Tests initialising to the earliest offset.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testInitialOffsetEarliest() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;

    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber subscriber =
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withInitialOffsetScheme(InitialOffsetScheme.EARLIEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));

    final RecordBatch b = subscriber.poll(1_000);
    assertEquals(2, b.size());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests initialising to the latest offest.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testInitialOffsetLatest() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withInitialOffsetScheme(InitialOffsetScheme.LATEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));

    final RecordBatch b = s.poll(10);
    assertEquals(0, b.size());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests the {@link InitialOffsetScheme#NONE} offset initialisation, which is only allowed in
   *  a group-aware context.
   *  
   *  @throws InterruptedException
   */
  @Test(expected=InvalidInitialOffsetSchemeException.class)
  public void testInitialOffsetNone() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig()
                        .withInitialOffsetScheme(InitialOffsetScheme.NONE)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
  }
  
  /**
   *  Tests ability to asynchronously consume messages from a {@link Receiver}.
   */
  @Test
  public void testReceiverConsume() {
    final String stream = "s";
    final int capacity = 10;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final RecordHandler handler = mock(RecordHandler.class);
    createReceiver(s, handler, 1_000);
    
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    wait.until(() -> {
      try {
        verify(handler, times(2)).onRecord(isNotNull());
      } catch (InterruptedException e) {}
    });
    verifyNoError(eh);
  }
}