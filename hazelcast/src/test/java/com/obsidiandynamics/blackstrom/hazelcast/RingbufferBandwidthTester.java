package com.obsidiandynamics.blackstrom.hazelcast;

import java.util.concurrent.*;
import java.util.function.*;

import org.slf4j.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.worker.*;

public class RingbufferBandwidthTester {
  private static final Logger log = LoggerFactory.getLogger(RingbufferBandwidthTester.class);
  
  private final int messages;
  
  private RingbufferBandwidthTester(int messages) {
    this.messages = messages;
  }
  
  private class TestPublisher {
    private final int pubIntervalMillis;
    private final byte[] bytes;
    private final Ringbuffer<byte[]> buffer;
    private int published;
    
    TestPublisher(Supplier<HazelcastInstance> instanceMaker, int pubIntervalMillis, int bytes) {
      this.pubIntervalMillis = pubIntervalMillis;
      this.bytes = new byte[bytes];
      buffer = instanceMaker.get().getRingbuffer("buffer");
      
      WorkerThread.builder()
      .withOptions(new WorkerOptions().daemon().withName(TestPublisher.class))
      .onCycle(this::publishCycle)
      .buildAndStart();
    }
    
    private void publishCycle(WorkerThread t) throws InterruptedException {
      buffer.addAsync(bytes, OverflowPolicy.OVERWRITE);
      published++;
      log.info("Published {}", published);
      
      if (published == messages) {
        log.info("Publisher: terminating");
        t.terminate();
      } else {
        Thread.sleep(pubIntervalMillis);
      }
    }
  }
  
  private class TestSubscriber {
    private final int pollTimeoutMillis;
    private final Ringbuffer<byte[]> buffer;
    private int received;
    private long nextSequence;
    
    TestSubscriber(Supplier<HazelcastInstance> instanceMaker, int pollTimeoutMillis) {
      this.pollTimeoutMillis = pollTimeoutMillis;
      buffer = instanceMaker.get().getRingbuffer("buffer");
      
      WorkerThread.builder()
      .withOptions(new WorkerOptions().daemon().withName(TestSubscriber.class))
      .onCycle(this::receiveCycle)
      .buildAndStart();
    }
    
    private void receiveCycle(WorkerThread t) throws InterruptedException {
      final ICompletableFuture<ReadResultSet<byte[]>> f = buffer.readManyAsync(nextSequence, 1, 1000, bytes -> bytes != null);
      try {
        final ReadResultSet<byte[]> results = f.get(pollTimeoutMillis, TimeUnit.MILLISECONDS);
        nextSequence = results.getSequence(results.size() - 1) + 1;
        received += results.size();
        log.info("Received {} records (total {})", results.size(), received);
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        log.info("Timed out");
      }
      
      if (received == messages) {
        log.info("Subscriber: terminating");
        t.terminate();
      }
    }
  }
  
  public static void main(String[] args) {
    final int messages = 1_000;
    final int pubIntervalMillis = 100;
    final int bytes = 10;
    final int pollTimeoutMillis = 1_000;
    
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none")
        .setProperty("hazelcast.shutdownhook.enabled", "false")
        .setNetworkConfig(new NetworkConfig()
                          .setJoin(new JoinConfig()
                                   .setMulticastConfig(new MulticastConfig()
                                                       .setEnabled(true)
                                                       .setMulticastTimeoutSeconds(1))
                                   .setTcpIpConfig(new TcpIpConfig()
                                                   .setEnabled(false))))
        .addRingBufferConfig(new RingbufferConfig().setName("default")
                             .setBackupCount(0)
                             .setAsyncBackupCount(0));

    final InstancePool instancePool = new InstancePool(3, () -> GridHazelcastProvider.getInstance().createInstance(config));
    log.info("Prestarting instances...");
    instancePool.prestartAll();
    log.info("Instances prestarted");
    
    new RingbufferBandwidthTester(messages) {{
      new TestPublisher(instancePool::get, pubIntervalMillis, bytes);
      new TestSubscriber(instancePool::get, pollTimeoutMillis);
      new TestSubscriber(instancePool::get, pollTimeoutMillis);
    }};
  }
}
