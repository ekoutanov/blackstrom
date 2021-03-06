package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPSC (multi-producer, single-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class SingleNodeQueueLedger implements Ledger {
  private static final int POLL_BACKOFF_MILLIS = 1;
  
  public static final class Config {
    int maxYields = 100;
    
    public Config withMaxYields(int maxYields) {
      this.maxYields = maxYields;
      return this;
    }
  }
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new HashSet<>();
  
  private volatile MessageHandler[] handlers = new MessageHandler[0];
  
  private final MessageContext context = new DefaultMessageContext(this, null, NopRetention.getInstance());
  
  private final WorkerThread thread;
  
  private final NodeQueue<Message> queue = new NodeQueue<>();
  
  private final QueueConsumer<Message> consumer = queue.consumer();
  
  private final int maxYields;
  
  private int yields;
  
  public SingleNodeQueueLedger() {
    this(new Config());
  }
  
  public SingleNodeQueueLedger(Config config) {
    maxYields = config.maxYields;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .daemon()
                     .withName(SingleNodeQueueLedger.class, Integer.toHexString(System.identityHashCode(this))))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final Message m = consumer.poll();
    if (m != null) {
      for (MessageHandler handler : handlers) {
        handler.onMessage(context, m);
      }
    } else if (yields++ < maxYields) {
      Thread.yield();
    } else {
      // resetting yields here appears counterintuitive (it makes more sense to reset it on a hit than a miss),
      // however, this technique avoids writing to an instance field from a hotspot, markedly improving performance
      // at the expense of (1) prematurely sleeping on the next miss and (2) yielding after a sleep
      yields = 0;
      Thread.sleep(POLL_BACKOFF_MILLIS);
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    final List<MessageHandler> handlersList = new ArrayList<>(Arrays.asList(handlers));
    handlersList.add(handler);
    handlers = handlersList.toArray(new MessageHandler[handlersList.size()]);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    queue.add(message);
    callback.onAppend(message.getMessageId(), null);
  }

  @Override
  public void dispose() {
    thread.terminate().joinSilently();
  }
}
