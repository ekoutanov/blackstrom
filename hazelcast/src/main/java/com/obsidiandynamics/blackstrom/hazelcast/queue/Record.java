package com.obsidiandynamics.blackstrom.hazelcast.queue;

public final class Record {
  public static final long UNASSIGNED_OFFSET = -1;
  
  private long offset = UNASSIGNED_OFFSET;
  
  private final byte[] data;
  
  public Record(byte[] data) {
    this.data = data;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return Record.class.getSimpleName() + " [offset=" + offset + ", data.length=" + data.length + "]";
  }
}
