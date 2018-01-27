package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.BalancedLedgerHub.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class BalancedLedgerView implements Ledger {
  private final BalancedLedgerHub hub;
  
  private class Consumer {
    private static final int CONSUME_WAIT_MILLIS = 1;
    
    private final MessageHandler handler;
    private final ConsumerGroup group;
    private final Object handlerId = UUID.randomUUID();
    private final WorkerThread thread;
    private final MessageContext context = new DefaultMessageContext(BalancedLedgerView.this, handlerId);
    private long[] nextReadOffsets = new long[accumulators.length];
    
    private final List<Message> sink = new ArrayList<>();

    Consumer(MessageHandler handler, ConsumerGroup group) {
      this.handler = handler;
      this.group = group;
      if (group != null) {
        group.join(handlerId);
      } else {
        Arrays.setAll(nextReadOffsets, shard -> accumulators[shard].getNextOffset());
      }
      
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().withDaemon(true).withName(BalancedLedgerView.class.getSimpleName() + "-" + handlerId))
          .onCycle(this::cycle)
          .buildAndStart();
    }
    
    private void cycle(WorkerThread t) throws InterruptedException {
      for (int shard = 0; shard < accumulators.length; shard++) {
        if (group == null || group.isAssignee(shard, handlerId)) {
          final Accumulator accumulator = accumulators[shard];
          final long nextReadOffset;
          if (group != null) {
            nextReadOffset = Math.max(nextReadOffsets[shard], group.getReadOffset(shard));
          } else {
            nextReadOffset = nextReadOffsets[shard];
          }
          
          final int retrieved = accumulator.retrieve(nextReadOffset, sink);
          if (retrieved != 0) {
            nextReadOffsets[shard] = ((ShardMessageId) sink.get(sink.size() - 1).getMessageId()).getOffset() + 1;
          }
        }
      }
      
      if (! sink.isEmpty()) {
        for (Message message : sink) {
          handler.onMessage(context, message);
        }
        sink.clear();
      } else {
        Thread.sleep(CONSUME_WAIT_MILLIS);
      }
    }
  }
  
  private final Map<Object, Consumer> consumers = new HashMap<>();
  
  private final Accumulator[] accumulators;
  
  BalancedLedgerView(BalancedLedgerHub hub) {
    this.hub = hub;
    accumulators = hub.getAccumulators(); 
  }
  
  public BalancedLedgerHub getHub() {
    return hub;
  }

  @Override
  public void attach(MessageHandler handler) {
    final ConsumerGroup group = handler.getGroupId() != null ? hub.getOrCreateGroup(handler.getGroupId()) : null;
    final Consumer consumer = new Consumer(handler, group);
    consumers.put(consumer.handlerId, consumer);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    hub.append(message, callback);
    callback.onAppend(message.getMessageId(), null);
  }

  @Override
  public void confirm(Object handlerId, Object messageId) {
    final ConsumerGroup group = consumers.get(handlerId).group;
    if (group != null) {
      final ShardMessageId shardMessageId = Cast.from(messageId);
      group.confirm(shardMessageId.getShard(), shardMessageId.getOffset());
    }
  }

  @Override
  public synchronized void dispose() {
    hub.detachView(this);
    final Collection<Consumer> consumers = this.consumers.values();
    consumers.forEach(c -> c.thread.terminate());
    consumers.forEach(c -> c.thread.joinQuietly());
    consumers.stream().filter(c -> c.group != null).forEach(c -> c.group.leave(c.handlerId));
    this.consumers.clear();
  }
}
