package com.obsidiandynamics.blackstrom.tracer;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class ShardedTracer implements Disposable {
  private static class ConfirmTask implements Runnable {
    private final MessageContext context;
    private final Object messageId;
    
    ConfirmTask(MessageContext context, Object messageId) {
      this.context = context;
      this.messageId = messageId;
    }

    @Override
    public void run() {
      context.confirm(messageId);
    }
  }
  
  private volatile Tracer[] tracers;
  
  private final Object lock = new Object();
  
  public ShardedTracer() {
    this(128);
  }
  
  public ShardedTracer(int stripes) {
    tracers = new Tracer[stripes];
  }
  
  public Action begin(MessageContext context, Message message) {
    final Tracer tracer;
    final int stripe = getStripe(message.getShard());
    final Tracer existingTracer = tracers[stripe];
    if (existingTracer != null) {
      tracer = existingTracer;
    } else {
      tracer = getOrSetTracer(stripe);
    }
    
    return tracer.begin(new ConfirmTask(context, message.getMessageId()));
  }
  
  private Tracer getOrSetTracer(int stripe) {
    synchronized (lock) {
      final Tracer existingTracer = tracers[stripe];
      if (existingTracer != null) {
        return existingTracer;
      } else {
        final Tracer newTracer = new Tracer(LazyFiringStrategy::new, Tracer.class.getSimpleName() + "-stripe-[" + stripe + "]");
        tracers[stripe] = newTracer;
        tracers = tracers; // volatile piggyback
        return newTracer;
      }
    }
  }
  
  private int getStripe(int shard) {
    final int nonNegativeUnboundedStripe = shard < 0 ? shard + Integer.MAX_VALUE : shard;
    return nonNegativeUnboundedStripe % tracers.length;
  }

  @Override
  public void dispose() {
    Arrays.stream(tracers).filter(t -> t != null).forEach(t -> t.terminate());
    Arrays.stream(tracers).filter(t -> t != null).forEach(t -> t.joinQuietly());
  }
}
