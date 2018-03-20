package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.util.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class PubSubOneWayTest extends AbstractPubSubTest {
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  @Test
  public void testOneWay() {
    testOneWay(2, 4, 10_000 * SCALE, 10, new InstancePool(2, this::newInstance), new OneWayOptions());
  }
  
  @Test
  public void testOneWayBenchmark() {
    Testmark.ifEnabled("one-way over grid", () -> {
      final OneWayOptions options = new OneWayOptions() {{
        verbose = true;
        printBacklog = false;
      }};
      final Supplier<InstancePool> poolSupplier = () -> new InstancePool(4, this::newGridInstance);
      final int messageSize = 100;
      
      testOneWay(1, 1, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(1, 2, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(1, 4, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(2, 4, 1_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(2, 8, 1_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(4, 8, 500_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(4, 16, 500_000 * SCALE, messageSize, poolSupplier.get(), options);
    });
  }
  
  private static class OneWayOptions {
    boolean verbose;
    boolean printBacklog;
  }
  
  private void testOneWay(int publishers, int subscribers, int messagesPerPublisher, int messageSize, 
                          InstancePool instancePool, OneWayOptions options) {
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final String stream = "s";
    final byte[] message = new byte[messageSize];
    final int capacity = backlogTarget * publishers * 2;
    final int pollTimeoutMillis = 100;
    
    // common configuration
    final StreamConfig streamConfig = new StreamConfig()
        .withName(stream)
        .withHeapCapacity(capacity);
    
    if (options.verbose) System.out.format("Prestarting instances for %d/%d pub/sub... ", publishers, subscribers);
    final int prestartInstances = Math.min(publishers + subscribers, instancePool.size());
    instancePool.prestart(prestartInstances);
    if (options.verbose) System.out.format("ready (x%d). Starting run...\n", prestartInstances);

    // create subscribers with receivers
    final ErrorHandler eh = mockErrorHandler();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withErrorHandler(eh)
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
        .withStreamConfig(streamConfig);
    
    final AtomicLong[] receivedArray = new AtomicLong[subscribers];
    for (int i = 0; i < subscribers; i++) {
      final AtomicLong received = new AtomicLong();
      receivedArray[i] = received;
      
      final HazelcastInstance instance = instancePool.get();
      final Subscriber s = configureSubscriber(instance, subConfig);
      createReceiver(s, record -> received.incrementAndGet(), pollTimeoutMillis);
    }
    
    final LongSupplier totalReceived = () -> {
      long total = 0;
      for (AtomicLong received : receivedArray) {
        total += received.get();
      }
      return total;
    };
    
    final LongSupplier smallestReceived = () -> {
      long smallest = Long.MAX_VALUE;
      for (AtomicLong received : receivedArray) {
        final long r = received.get();
        if (r < smallest) {
          smallest = r;
        }
      }
      return smallest;
    };
    
    // create the publishers and send across several threads
    final PublisherConfig pubConfig = new PublisherConfig()
        .withStreamConfig(streamConfig);
    final List<Publisher> publishersList = IntStream.range(0, publishers).boxed()
        .map(i -> configurePublisher(instancePool.get(), pubConfig)).collect(Collectors.toList());
    
    final AtomicLong totalSent = new AtomicLong();
    final long took = TestSupport.took(() -> {
      ParallelJob.blocking(publishers, threadNo -> {
        final Publisher p = publishersList.get(threadNo);
        
        for (int i = 0; i < messagesPerPublisher; i++) {
          p.publishAsync(new Record(message), PublishCallback.nop());
          
          if (i != 0 && i % checkInterval == 0) {
            long lastLogTime = 0;
            final long sent = totalSent.addAndGet(checkInterval);
            for (;;) {
              final int backlog = (int) (sent - smallestReceived.getAsLong());
              if (backlog >= backlogTarget) {
                TestSupport.sleep(1);
                if (options.printBacklog && System.currentTimeMillis() - lastLogTime > 5_000) {
                  TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d messages)\n", backlog, sent);
                  lastLogTime = System.currentTimeMillis();
                }
              } else {
                break;
              }
            }
          }
        }
      }).run();

      wait.until(() -> {
        assertEquals(publishers * messagesPerPublisher * (long) subscribers, totalReceived.getAsLong());
      });
    });
                                     
    final long totalMessages = (long) publishers * messagesPerPublisher * subscribers;
    final double rate = (double) totalMessages / took * 1000;
    final long bps = (long) (rate * messageSize * 8 * 2);
    
    if (options.verbose) {
      System.out.format("%,d msgs took %,d ms, %,.0f msg/s, %s\n", totalMessages, took, rate, Bandwidth.translate(bps));
    }
    verifyNoError(eh);
    
    afterBase();
    beforeBase();
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(PubSubOneWayTest.class);
  }
}
