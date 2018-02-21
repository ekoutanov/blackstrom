package com.obsidiandynamics.blackstrom.group;

import java.io.*;

public final class ResponseSync {
  private final Group group;
  private final Serializable id;
  private final HostMessageHandler handler;
  
  ResponseSync(Group group, Serializable id, HostMessageHandler handler) {
    this.group = group;
    this.id = id;
    this.handler = handler;
  }
  
  public void cancel() {
    group.removeHandler(id, handler);
  }

  @Override
  public String toString() {
    return ResponseSync.class.getSimpleName() + "[id=" + id + "]";
  }
}
