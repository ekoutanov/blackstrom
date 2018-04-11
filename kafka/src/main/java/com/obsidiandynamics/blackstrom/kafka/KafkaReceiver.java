package com.obsidiandynamics.blackstrom.kafka;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.*;

import com.obsidiandynamics.worker.*;

public final class KafkaReceiver<K, V> implements Terminable, Joinable {
  @FunctionalInterface
  public interface RecordHandler<K, V> {
    void onReceive(ConsumerRecords<K, V> records) throws InterruptedException;
  }
  
  @FunctionalInterface
  public interface ErrorHandler {
    void onError(Throwable cause);
  }
  
  private final Consumer<K, V> consumer;
  
  private final int pollTimeoutMillis;
  
  private final RecordHandler<K, V> recordHandler;
  
  private final ErrorHandler errorHandler;
  
  private final WorkerThread thread;
  
  public static ErrorHandler genericErrorLogger(Logger logger) {
    return cause -> logger.warn("Error processing Kafka record", cause);
  }
  
  public KafkaReceiver(Consumer<K, V> consumer, int pollTimeoutMillis, String threadName, 
                       RecordHandler<K, V> recordHandler, ErrorHandler errorHandler) {
    this.consumer = consumer;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.recordHandler = recordHandler;
    this.errorHandler = errorHandler;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(threadName))
        .onCycle(this::cycle)
        .onShutdown(this::shutdown)
        .buildAndStart();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final ConsumerRecords<K, V> records;
    try {
      records = consumer.poll(pollTimeoutMillis);
      
      if (! records.isEmpty()) {
        recordHandler.onReceive(records);
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (org.apache.kafka.common.errors.InterruptException e) {
      throw new InterruptedException("Converted from " + org.apache.kafka.common.errors.InterruptException.class.getName());
    } catch (Throwable e) {
      errorHandler.onError(e);
      return;
    }
  }
  
  private void shutdown(WorkerThread thread, Throwable exception) {
    consumer.close();
  }
  
  @Override
  public Joinable terminate() {
    return thread.terminate();
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
}
