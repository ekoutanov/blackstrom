package com.obsidiandynamics.blackstrom.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class ProducerPipe<K, V> implements Terminable, Joinable {
  private static final int MAX_YIELDS = 100;
  private static final int QUEUE_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord<K, V> {
    final ProducerRecord<K, V> record;
    final Callback callback;
    
    AsyncRecord(ProducerRecord<K, V> record, Callback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<AsyncRecord<K, V>> queue = new NodeQueue<>();
  
  private final QueueConsumer<AsyncRecord<K, V>> queueConsumer = queue.consumer();
  
  private final Producer<K, V> producer;
  
  private final WorkerThread thread;
  
  private final Logger log;
  
  private int yields;
  
  private volatile boolean producerDisposed;
  
  public ProducerPipe(ProducerPipeConfig config, Producer<K, V> producer, String threadName, Logger log) {
    this.producer = producer;
    this.log = log;
    if (config.isAsync()) {
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(threadName))
          .onCycle(this::cycle)
          .buildAndStart();
    } else {
      thread = null;
    }
  }
  
  public void send(ProducerRecord<K, V> record, Callback callback) {
    if (thread != null) {
      queue.add(new AsyncRecord<>(record, callback));
    } else {
      sendNow(record, callback);
    }
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord<K, V> rec = queueConsumer.poll();
    if (rec != null) {
      sendNow(rec.record, rec.callback);
      yields = 0;
    } else if (yields++ < MAX_YIELDS) {
      Thread.yield();
    } else {
      Thread.sleep(QUEUE_BACKOFF_MILLIS);
    }
  }
  
  private void sendNow(ProducerRecord<K, V> record, Callback callback) {
    try {
      producer.send(record, callback);
    } catch (Throwable e) {
      if (! producerDisposed) {
        log.error(String.format("Error sending %s", record), e);
      }
    }
  }
  
  @Override
  public Joinable terminate() {
    if (thread != null) thread.terminate();
    closeProducer();
    return this;
  }
  
  void closeProducer() {
    producerDisposed = true;
    producer.close();
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread != null ? thread.join(timeoutMillis) : true;
  }
}
