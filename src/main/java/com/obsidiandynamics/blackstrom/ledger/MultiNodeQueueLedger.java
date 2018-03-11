package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPMC (multi-producer, 
-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class MultiNodeQueueLedger implements Ledger {
  private static final int POLL_BACKOFF_MILLIS = 1;
  
  public static final class Config {
    int maxYields = 100;
    
    int debugMessageCounts = 0;
    
    public Config withMaxYields(int maxYields) {
      this.maxYields = maxYields;
      return this;
    }
    
    public Config withDebugMessageCounts(int debugMessageCounts) {
      this.debugMessageCounts = debugMessageCounts;
      return this;
    }
  }
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new HashSet<>();
  
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  
  private final MessageContext context = new DefaultMessageContext(this, null, NopRetention.getInstance());

  private final NodeQueue<Message> queue = new NodeQueue<>();
  
  private final int debugMessageCounts;
  
  private final int maxYields;
  
  private class NodeWorker implements WorkerCycle {
    private final MessageHandler handler;
    private final QueueConsumer<Message> consumer;
    private final String groupId;
    private int yields;
    
    NodeWorker(MessageHandler handler, String groupId, QueueConsumer<Message> consumer) {
      this.handler = handler;
      this.groupId = groupId;
      this.consumer = consumer;
    }
    
    private final AtomicLong consumed = new AtomicLong();
    
    @Override
    public void cycle(WorkerThread thread) throws InterruptedException {
      final Message m = consumer.poll();
      if (m != null) {
        if (debugMessageCounts != 0) {
          final long consumed = this.consumed.getAndIncrement();
          if (consumed % debugMessageCounts == 0) {
            System.out.format("groupId=%s, consumed=%,d \n", groupId, consumed);
          }
        }
        
        handler.onMessage(context, m);
      } else if (yields++ < maxYields) {
        Thread.yield();
      } else {
        yields = 0;
        Thread.sleep(POLL_BACKOFF_MILLIS);
      }
    }
  }
  
  public MultiNodeQueueLedger() {
    this(new Config());
  }
  
  public MultiNodeQueueLedger(Config config) {
    maxYields = config.maxYields;
    debugMessageCounts = config.debugMessageCounts;
  }
  
  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(MultiNodeQueueLedger.class.getSimpleName() + "-" + handler.getGroupId())
                     .withDaemon(true))
        .onCycle(new NodeWorker(handler, handler.getGroupId(), queue.consumer()))
        .buildAndStart();
    threads.add(thread);
  }
  
  private final AtomicLong appends = new AtomicLong();
  
  @Override
  public void append(Message message, AppendCallback callback) {
    if (debugMessageCounts != 0) {
      final long appends = this.appends.getAndIncrement();
      if (appends % debugMessageCounts == 0) {
        System.out.format("appends=%,d\n", appends);
      }
    }
    
    queue.add(message);
    callback.onAppend(message.getMessageId(), null);
  }
  
  @Override
  public void dispose() {
    threads.forEach(t -> t.terminate());
    threads.forEach(t -> t.joinQuietly());
  }
}
