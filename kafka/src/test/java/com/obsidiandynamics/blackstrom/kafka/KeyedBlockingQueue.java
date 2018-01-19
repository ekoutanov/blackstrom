package com.obsidiandynamics.blackstrom.kafka;

import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

import com.obsidiandynamics.blackstrom.keyed.*;

public final class KeyedBlockingQueue<K, E> extends Keyed<K, BlockingQueue<E>> {
  public KeyedBlockingQueue(Supplier<BlockingQueue<E>> partitionFactory) {
    super(partitionFactory);
  }

  public int totalSize() {
    return map.values().stream().collect(Collectors.summingInt(q -> q.size())).intValue();
  }
}
