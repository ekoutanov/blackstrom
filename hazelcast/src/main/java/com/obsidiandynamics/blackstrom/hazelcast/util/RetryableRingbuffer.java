package com.obsidiandynamics.blackstrom.hazelcast.util;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class RetryableRingbuffer<E> {
  private final Retry retry;
  
  private final Ringbuffer<E> ringbuffer;

  public RetryableRingbuffer(Retry retry, Ringbuffer<E> ringbuffer) {
    this.retry = retry;
    this.ringbuffer = ringbuffer;
  }
  
  public Ringbuffer<E> getRingbuffer() {
    return ringbuffer;
  }

  public ICompletableFuture<Long> addAsync(E item, OverflowPolicy overflowPolicy) {
    return retry.run(() -> ringbuffer.addAsync(item, overflowPolicy));
  }

  public long add(E item) {
    return retry.run(() -> ringbuffer.add(item));
  }

  public ICompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, int minCount, int maxCount, IFunction<E, Boolean> filter) {
    return retry.run(() -> ringbuffer.readManyAsync(startSequence, minCount, maxCount, filter));
  }

  public long tailSequence() {
    return retry.run(() -> ringbuffer.tailSequence());
  }
}
