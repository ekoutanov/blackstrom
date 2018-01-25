package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.model.*;

final class ArrayListAccumulator implements Accumulator {
  private static class Buffer {
    private final Buffer previous;
    private final long baseOffset;
    private final List<Message> items = new CopyOnWriteArrayList<>();
    private volatile Buffer next;
    
    Buffer(Buffer previous, long baseOffset) {
      this.previous = previous;
      this.baseOffset = baseOffset;
    }
    
    int retrieve(long fromOffset, List<Message> sink) {
      final int length = items.size();
      final int startIndex;
      if (fromOffset > baseOffset) {
        startIndex = (int) Math.min(fromOffset - baseOffset, length);
      } else {
        startIndex = 0;
      }
      for (int i = startIndex; i < length; i++) {
        sink.add(items.get(i));
      }
      return length - startIndex;
    }
  }
//  private static class Buffer {
//    private final Buffer previous;
//    private final long baseOffset;
//    private final BlockingQueue<Message> items = new LinkedBlockingQueue<>();
//    private volatile Buffer next;
//    
//    Buffer(Buffer previous, long baseOffset) {
//      this.previous = previous;
//      this.baseOffset = baseOffset;
//    }
//    
//    int retrieve(long fromOffset, List<Message> sink) {
//      final int length = items.size();
//      final int startIndex;
//      if (fromOffset > baseOffset) {
//        startIndex = (int) Math.min(fromOffset - baseOffset, length);
//      } else {
//        startIndex = 0;
//      }
//      
//      int skipped = 0;
//      int added = 0;
//      for (Message m : items) {
//        if (skipped == startIndex) {
//          sink.add(m);
//          added++;
//        } else {
//          skipped++;
//        }
//      }
//      return added;
//    }
//  }
//  private class Buffer {
//    private final Buffer previous;
//    private final long baseOffset;
//    private final BlockingQueue<Message> items = new ArrayBlockingQueue<>(bufferSize);
//    private volatile Buffer next;
//    
//    Buffer(Buffer previous, long baseOffset) {
//      this.previous = previous;
//      this.baseOffset = baseOffset;
//    }
//    
//    int retrieve(long fromOffset, List<Message> sink) {
//      final int startIndex;
//      if (fromOffset > baseOffset) {
//        startIndex = (int) (fromOffset - baseOffset);
//      } else {
//        startIndex = 0;
//      }
//      
//      int skipped = 0;
//      int added = 0;
//      for (Message m : items) {
//        if (skipped == startIndex) {
//          sink.add(m);
//          added++;
//        } else {
//          skipped++;
//        }
//      }
//      return added;
//    }
//  }
//  
  private final int shard;
  private final int bufferSize;
  private volatile long nextOffset;
  private volatile Buffer latest;
  private final Object lock = new Object();
  
  ArrayListAccumulator(int shard, int bufferSize, long baseOffset) {
    this.shard = shard;
    this.bufferSize = bufferSize;
    latest = new Buffer(null, baseOffset);
    nextOffset = baseOffset;
  }

  @Override
  public void append(Message message) {
    synchronized (lock) {
      if (latest.items.size() == bufferSize) {
        createNextBuffer();
      }
      final ShardMessageId messageId = new ShardMessageId(shard, nextOffset);
      message.withMessageId(messageId);
      latest.items.add(message);
      nextOffset++;
    }
  }
  
  private void createNextBuffer() {
    final Buffer next = new Buffer(latest, latest.baseOffset + latest.items.size());
    latest.next = next;
    latest = next;
  }

  @Override
  public long getNextOffset() {
    return nextOffset;
  }

  @Override
  public int retrieve(long fromOffset, List<Message> sink) {
    Buffer buffer = findBuffer(fromOffset);
    int totalRetrieved = 0;
    while (buffer != null) {
      final boolean bufferFull = buffer.next != null;
      totalRetrieved += buffer.retrieve(fromOffset, sink);
      if (bufferFull) {
        buffer = buffer.next;
      } else {
        break;
      }
    }
    return totalRetrieved;
  }
  
  private Buffer findBuffer(long offset) {
    Buffer buffer = latest;
    while (buffer.baseOffset > offset && buffer.previous != null) {
      buffer = buffer.previous;
    }
    return buffer;
  }
  
  static Accumulator.Factory factory(int bufferSize) {
    return shard -> new ArrayListAccumulator(shard, bufferSize, 0);
  }
}
