package com.obsidiandynamics.blackstrom.hazelcast;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.slf4j.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.worker.*;

public class RingbufferFailoverSim {
  private static final Logger log = LoggerFactory.getLogger(RingbufferFailoverSim.class);
  
  private static final String BUFFER_NAME = "buffer0";
  
  private final int messages;
  
  private RingbufferFailoverSim(int messages) {
    this.messages = messages;
  }
  
  private class TestPublisher implements Joinable {
    private final int pubIntervalMillis;
    private final byte[] bytes;
    private final Ringbuffer<byte[]> buffer;
    private final WorkerThread thread;
    private int published;
    
    TestPublisher(Supplier<HazelcastInstance> instanceMaker, int pubIntervalMillis, int bytes) {
      this.pubIntervalMillis = pubIntervalMillis;
      this.bytes = new byte[bytes];
      final HazelcastInstance instance = instanceMaker.get();
      buffer = instance.getRingbuffer(BUFFER_NAME);
      log.info("serviceName={}, partitionKey={}", buffer.getServiceName(), buffer.getPartitionKey());
      final Partition partition = instance.getPartitionService().getPartition(BUFFER_NAME);
      log.info("partitionId={}, owner={}", partition.getPartitionId(), partition.getOwner());
      instance.getPartitionService().addMigrationListener(new MigrationListener() {
        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
          log.info("Migration started {}", migrationEvent);
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
          log.info("Migration compeleted {}", migrationEvent);
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
          log.info("Migration failed {}", migrationEvent);
        }
      });
      
      thread = WorkerThread.builder()
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

    @Override
    public boolean join(long timeoutMillis) throws InterruptedException {
      return thread.join(timeoutMillis);
    }
  }
  
  private class TestSubscriber implements Joinable {
    private final int pollTimeoutMillis;
    private final Ringbuffer<byte[]> buffer;
    private final boolean terminateOnComplete;
    private final WorkerThread thread;
    private int received;
    private long nextSequence;
    
    TestSubscriber(Supplier<HazelcastInstance> instanceMaker, int pollTimeoutMillis, boolean terminateOnComplete) {
      this.pollTimeoutMillis = pollTimeoutMillis;
      this.terminateOnComplete = terminateOnComplete;
      buffer = instanceMaker.get().getRingbuffer(BUFFER_NAME);
      
      thread = WorkerThread.builder()
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
        log.info("Received {} records (total {}) next is {}", results.size(), received, nextSequence);
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        log.info("Timed out");
      }
      
      if (received % messages == 0 && received > 0) {
        log.info("Subscriber: received one complete set");
        if (terminateOnComplete) {
          t.terminate();
        }
      }
    }

    @Override
    public boolean join(long timeoutMillis) throws InterruptedException {
      return thread.join(timeoutMillis);
    }
  }
  
  public static void main(String[] args) {
    final int messages = 100;
    final int pubIntervalMillis = 10;
    final int bytes = 10;
    final int pollTimeoutMillis = 1_000;
    
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none")
        .setProperty("hazelcast.shutdownhook.enabled", "false")
        .setProperty("hazelcast.graceful.shutdown.max.wait", String.valueOf(5))
        .setProperty("hazelcast.wait.seconds.before.join", String.valueOf(0))
        .setProperty("hazelcast.max.wait.seconds.before.join", String.valueOf(0))
        .setNetworkConfig(new NetworkConfig()
                          .setJoin(new JoinConfig()
                                   .setMulticastConfig(new MulticastConfig()
                                                       .setEnabled(true)
                                                       .setMulticastTimeoutSeconds(1))
                                   .setTcpIpConfig(new TcpIpConfig()
                                                   .setEnabled(false))))
        .addRingBufferConfig(new RingbufferConfig()
                             .setName("default")
                             .setBackupCount(0)
                             .setAsyncBackupCount(0));

    final Supplier<HazelcastInstance> instanceSupplier = () -> GridHazelcastProvider.getInstance().createInstance(config);

    log.info("Creating publisher instance...");
    final AtomicReference<HazelcastInstance> instance = new AtomicReference<>(instanceSupplier.get());
    instance.get().getRingbuffer(BUFFER_NAME);
    
    final InstancePool instancePool = new InstancePool(2, instanceSupplier);
    log.info("Prestarting subscriber instances...");
    instancePool.prestartAll();
    log.info("Instances prestarted");
    
    final int runs = 5;
    new RingbufferFailoverSim(messages) {{
      new TestSubscriber(instancePool::get, pollTimeoutMillis, false);
      new TestSubscriber(instancePool::get, pollTimeoutMillis, false);
      
      for (int i = 0; i < runs; i++) {
        if (instance.get() == null) {
          log.info("Creating publisher instance...");
          instance.set(instanceSupplier.get());
        }
        
        log.info("Publisher instance created");
        final TestPublisher pub = new TestPublisher(instance::get, pubIntervalMillis, bytes);
        final TestSubscriber sub = new TestSubscriber(instance::get, pollTimeoutMillis, true);
        Joiner.of(pub, sub).joinSilently();
        instance.getAndSet(null).shutdown();
      }
    }};
  }
}
