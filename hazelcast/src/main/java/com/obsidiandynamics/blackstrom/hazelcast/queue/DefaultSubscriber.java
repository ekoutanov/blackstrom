package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.concurrent.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.util.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;
import com.obsidiandynamics.blackstrom.worker.*;
import com.obsidiandynamics.blackstrom.worker.Terminator;

public final class DefaultSubscriber implements Subscriber, Joinable {
  private static final int KEEPER_BACKOFF_MILLIS = 1;
  
  private final HazelcastInstance instance;
  
  private final SubscriberConfig config;
  
  private final RetryableRingbuffer<byte[]> buffer;
  
  private final RetryableMap<String, Long> offsets;
  
  private final Election election;
  
  private final UUID leaseCandidate;
  
  private final WorkerThread keeperThread;
  
  private volatile long nextReadOffset;
  
  private volatile long lastReadOffset;
  
  private volatile long scheduledConfirmOffset = Record.UNASSIGNED_OFFSET;
  
  private long lastConfirmedOffset = scheduledConfirmOffset;
  
  private volatile long scheduledExtendTimestamp = 0;
  
  private long lastExtendTimestamp = scheduledExtendTimestamp;
  
  private boolean active = true;
  
  private final Object activeLock = new Object();
  
  DefaultSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    this.instance = instance;
    this.config = config;
    
    final StreamConfig streamConfig = config.getStreamConfig();
    final Retry retry = new Retry()
        .withExceptionClass(HazelcastException.class)
        .withAttempts(Integer.MAX_VALUE)
        .withBackoffMillis(100)
        .withLog(config.getLog());
    buffer = new RetryableRingbuffer<>(retry, StreamHelper.getRingbuffer(instance, streamConfig));
    config.getLog().info("Subscriber: serviceName={}, partitionKey={}", 
                         buffer.getRingbuffer().getServiceName(), buffer.getRingbuffer().getPartitionKey());
    final Partition partition = instance.getPartitionService().getPartition(buffer.getRingbuffer().getName());
    config.getLog().info("Subscriber: partitionId={}, owner={}", partition.getPartitionId(), partition.getOwner());
    
    if (config.hasGroup()) {
      // checks for IllegalArgumentException; no initial assignment is made until poll() is called
      getInitialOffset(true);
      nextReadOffset = Record.UNASSIGNED_OFFSET;
      
      final String offsetsFQName = QNamespace.HAZELQ_META.qualify("offsets." + streamConfig.getName());
      offsets = new RetryableMap<>(retry, instance.getMap(offsetsFQName));
      
      final String leaseFQName = QNamespace.HAZELQ_META.qualify("lease." + streamConfig.getName());
      final IMap<String, byte[]> leaseTable = instance.getMap(leaseFQName);
      leaseCandidate = UUID.randomUUID();
      election = new Election(config.getElectionConfig(), leaseTable);
      election.getRegistry().enrol(config.getGroup(), leaseCandidate);
      
      keeperThread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(Subscriber.class, streamConfig.getName(), "keeper"))
          .onCycle(this::keeperCycle)
          .buildAndStart();
    } else {
      if (config.getInitialOffsetScheme() == InitialOffsetScheme.NONE) {
        throw new InvalidInitialOffsetSchemeException("Cannot use initial offset scheme " + InitialOffsetScheme.NONE + 
                                                      " in a group-free context");
      }
      // performs initial offset assignment
      nextReadOffset = getInitialOffset(false);
      offsets = null;
      election = null;
      leaseCandidate = null;
      keeperThread = null;
    }
    lastReadOffset = nextReadOffset - 1;
  }
  
  @Override
  public SubscriberConfig getConfig() {
    return config;
  }
  
  HazelcastInstance getInstance() {
    return instance;
  }
  
  Election getElection() {
    return election;
  }

  @Override
  public RecordBatch poll(long timeoutMillis) throws InterruptedException {
    final boolean isGroupSubscriber = leaseCandidate != null;
    final boolean isCurrentTenant = isGroupSubscriber && isCurrentTenant();
    
    if (! isGroupSubscriber || isCurrentTenant) {
      if (nextReadOffset == Record.UNASSIGNED_OFFSET) {
        nextReadOffset = loadConfirmedOffset() + 1;
        lastReadOffset = nextReadOffset - 1;
      }
      
      final ICompletableFuture<ReadResultSet<byte[]>> f = buffer.readManyAsync(nextReadOffset, 1, 1_000, StreamHelper::isNotNull);
      try {
        final ReadResultSet<byte[]> resultSet = f.get(timeoutMillis, TimeUnit.MILLISECONDS);
        lastReadOffset = resultSet.getSequence(resultSet.size() - 1);
        nextReadOffset = lastReadOffset + 1;
        return readBatch(resultSet);
      } catch (ExecutionException e) {
        final String m = String.format("Error reading at offset %d from stream %s",
                                       nextReadOffset, config.getStreamConfig().getName());
        config.getErrorHandler().onError(m, e);
        config.getLog().info("Subscriber: serviceName={}, partitionKey={}", 
                             buffer.getRingbuffer().getServiceName(), buffer.getRingbuffer().getPartitionKey());
        final Partition partition = instance.getPartitionService().getPartition(buffer.getRingbuffer().getName());
        config.getLog().info("Subscriber: partitionId={}, owner={}", partition.getPartitionId(), partition.getOwner());
        Thread.sleep(1000);//TODO
        f.cancel(true);
        return RecordBatch.empty();
      } catch (TimeoutException e) {
        f.cancel(true);
        return RecordBatch.empty();
      } finally {
        if (isCurrentTenant) {
          scheduledExtendTimestamp = System.currentTimeMillis();
        }
      }
    } else {
      nextReadOffset = Record.UNASSIGNED_OFFSET;
      Thread.sleep(timeoutMillis);
      return RecordBatch.empty();
    }
  }
  
  private long loadConfirmedOffset() {
    final Long confirmedOffset = offsets.get(config.getGroup());
    if (confirmedOffset != null) {
      return confirmedOffset;
    } else {
      return getInitialOffset(true) - 1;
    }
  }
  
  private long getInitialOffset(boolean useGroups) {
    // resolve AUTO to the appropriate scheme (EARLIEST/LATEST/NONE) depending on group mode
    final InitialOffsetScheme concreteInitialOffsetScheme = config.getInitialOffsetScheme().resolveConcreteScheme(useGroups);
    if (concreteInitialOffsetScheme == InitialOffsetScheme.EARLIEST) {
      return 0;
    } else if (concreteInitialOffsetScheme == InitialOffsetScheme.LATEST) {
      return buffer.tailSequence() + 1;
    } else {
      throw new OffsetLoadException("No persisted offset");
    }
  }
  
  private static RecordBatch readBatch(ReadResultSet<byte[]> resultSet) {
    final List<Record> records = new ArrayList<>(resultSet.size());
    long offset = resultSet.getSequence(0);
    for (byte[] result : resultSet) {
      records.add(new Record(result, offset++));
    }
    return new ListRecordBatch(records);
  }
  
  private void ensureGroupMode() {
    if (leaseCandidate == null) {
      throw new IllegalStateException("Cannot call this operation in a group-free context");
    }
  }
  
  private void ensureGroupFreeMode() {
    if (leaseCandidate != null) {
      throw new IllegalStateException("Cannot call this operation in a group-aware context");
    }
  }
  
  @Override
  public void confirm() {
    ensureGroupMode();
    
    if (lastReadOffset != Record.UNASSIGNED_OFFSET) {
      confirm(lastReadOffset);
    }
  }

  @Override
  public void confirm(long offset) {
    ensureGroupMode();
    
    if (offset < StreamHelper.SMALLEST_OFFSET || offset > lastReadOffset) {
      throw new IllegalArgumentException(String.format("Illegal offset %d; last read %d", offset, lastReadOffset));
    }
    
    scheduledConfirmOffset = offset;
  }
  
  @Override
  public void seek(long offset) {
    ensureGroupFreeMode();
    
    if (offset < StreamHelper.SMALLEST_OFFSET) throw new IllegalArgumentException("Invalid seek offset " + offset);
    nextReadOffset = offset;
  }
  
  private void keeperCycle(WorkerThread t) throws InterruptedException {
    final long scheduledConfirmOffset = this.scheduledConfirmOffset;
    final long scheduledExtendTimestamp = this.scheduledExtendTimestamp;
    
    boolean performedWork = false;
    synchronized (activeLock) {
      // avoid confirming offsets or extending the lease if this subscriber has been deactivated,
      // but update the timestamps to thwart future attempts
      if (active) {
        if (scheduledConfirmOffset != lastConfirmedOffset) {
          performedWork = true;
          putOffset(scheduledConfirmOffset);
        }
        
        if (scheduledExtendTimestamp != lastExtendTimestamp) {
          performedWork = true;
          extendLease(scheduledExtendTimestamp);
        }
      }

      lastConfirmedOffset = scheduledConfirmOffset;
      lastExtendTimestamp = scheduledExtendTimestamp;
    }
    
    if (! performedWork) {
      Thread.sleep(KEEPER_BACKOFF_MILLIS);
    }
  }
  
  private void putOffset(long offset) {
    if (isCurrentTenant()) {
      doWithErrorHandler(() -> offsets.put(config.getGroup(), offset), 
                         config.getErrorHandler(), 
                         "Failed to update offset");
    } else {
      final String m = String.format("Failed confirming offset %s for stream %s: %s is not the current tenant for group %s",
                                     offset, config.getStreamConfig().getName(), leaseCandidate, config.getGroup());
      config.getErrorHandler().onError(m, null);
    }
  }
  
  private void extendLease(long timestamp) {
    doWithErrorHandler(() -> election.extend(config.getGroup(), leaseCandidate), 
                       config.getErrorHandler(), 
                       "Failed to extend lease");
  }
  
  private boolean isCurrentTenant() {
    return election.getLeaseView().isCurrentTenant(config.getGroup(), leaseCandidate);
  }
  
  @Override
  public boolean isAssigned() {
    return leaseCandidate == null || isCurrentTenant();
  }
  
  @Override
  public void deactivate() {
    deactivate(config.getErrorHandler());
  }
  
  private void deactivate(ErrorHandler errorHandler) {
    ensureGroupMode();
    
    synchronized (activeLock) {
      election.getRegistry().unenrol(config.getGroup(), leaseCandidate);
      if (isCurrentTenant()) {
        doWithErrorHandler(() -> election.yield(config.getGroup(), leaseCandidate), 
                           errorHandler, 
                           "Failed to yield lease");
      }
      active = false;
    }
  }
  
  private static void doWithErrorHandler(CheckedRunnable<?> r, ErrorHandler errorHandler, String message) {
    try {
      r.run();
    } catch (Throwable e) {
      errorHandler.onError(message, e);
    }
  }
  
  @Override
  public void reactivate() {
    ensureGroupMode();
    
    synchronized (activeLock) {
      election.getRegistry().enrol(config.getGroup(), leaseCandidate);
      active = true;
    }
  }

  @Override
  public Joinable terminate() {
    if (leaseCandidate != null) {
      deactivate(ErrorHandler.nop());
    }
    
    Terminator.blank()
    .add(Optional.ofNullable(keeperThread))
    .add(Optional.ofNullable(election))
    .terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return Joiner.blank()
    .add(Optional.ofNullable(keeperThread))
    .add(Optional.ofNullable(election))
    .join(timeoutMillis);
  }
}
