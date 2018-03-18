package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultReceiver implements Receiver {
  private final Subscriber subscriber;
  
  private final RecordHandler recordHandler;
  
  private final int pollTimeoutMillis;
  
  private final WorkerThread pollerThread;
  
  DefaultReceiver(Subscriber subscriber, RecordHandler recordHandler, int pollTimeoutMillis) {
    this.subscriber = subscriber;
    this.recordHandler = recordHandler;
    this.pollTimeoutMillis = pollTimeoutMillis;
    pollerThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(DefaultReceiver.class, subscriber.getConfig().getStreamConfig().getName(), "poller"))
        .onCycle(this::pollerCycle)
        .buildAndStart();
  }
  
  private void pollerCycle(WorkerThread thread) throws InterruptedException {
    final RecordBatch batch = subscriber.poll(pollTimeoutMillis);
    for (Record record : batch) {
      recordHandler.onRecord(record);
    }
    Thread.sleep(pollTimeoutMillis);
  }
  
  @Override
  public Joinable terminate() {
    pollerThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return pollerThread.join(timeoutMillis);
  }
}
