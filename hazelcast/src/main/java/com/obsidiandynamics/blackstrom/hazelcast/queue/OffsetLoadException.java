package com.obsidiandynamics.blackstrom.hazelcast.queue;

public final class OffsetLoadException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  OffsetLoadException(String m) { super(m); }
}
