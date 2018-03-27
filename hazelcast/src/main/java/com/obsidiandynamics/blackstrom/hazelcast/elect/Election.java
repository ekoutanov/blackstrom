package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.util.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class Election implements Terminable, Joinable {
  private static final Logger log = LoggerFactory.getLogger(Election.class);
  
  private final ElectionConfig config;
  
  private final RetryableMap<String, byte[]> leases;
  
  private final Registry registry;
  
  private final WorkerThread scavengerThread;
  
  private final Object scavengeLock = new Object();
  
  private final Object viewLock = new Object();
  
  private volatile LeaseViewImpl leaseView = new LeaseViewImpl(0);
  
  private long nextViewVersion = 1;
  
  public Election(ElectionConfig config, IMap<String, byte[]> leases, Registry initialRegistry) {
    this.config = config;

    final Retry retry = new Retry()
        .withExceptionClass(HazelcastException.class)
        .withAttempts(Integer.MAX_VALUE)
        .withBackoffMillis(100)
        .withLog(log);
    this.leases = new RetryableMap<>(retry, leases);
    registry = new Registry(initialRegistry);
    
    scavengerThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(Election.class, "scavenger"))
        .onCycle(this::scavegerCycle)
        .buildAndStart();
  }
  
  public Registry getRegistry() {
    return registry;
  }
  
  private void scavegerCycle(WorkerThread t) throws InterruptedException {
    scavenge();
    Thread.sleep(config.getScavengeInterval());
  }
  
  void scavenge() {
    reloadView();
    
    synchronized (scavengeLock) {
      final Set<String> resources = registry.getCandidatesView().keySet();
      for (String resource : resources) {
        final Lease existingLease = leaseView.getOrDefault(resource, Lease.vacant());
        if (! existingLease.isCurrent()) {
          if (existingLease.isVacant()) {
            log.debug("Lease of {} is vacant", resource); 
          } else {
            config.getScavengeWatcher().onExpire(resource, existingLease.getTenant());
            log.debug("Lease of {} by {} expired at {}", resource, existingLease.getTenant(), Lease.formatExpiry(existingLease.getExpiry()));
          }
          
          final UUID nextCandidate = registry.getRandomCandidate(resource);
          if (nextCandidate != null) {
            final boolean success;
            final Lease newLease = new Lease(nextCandidate, System.currentTimeMillis() + config.getLeaseDuration());
            if (existingLease.isVacant()) {
              final byte[] previous = leases.putIfAbsent(resource, newLease.pack());
              success = previous == null;
            } else {
              success = leases.replace(resource, existingLease.pack(), newLease.pack());
            }
            
            if (success) {
              log.debug("New lease of {} by {} until {}", resource, nextCandidate, Lease.formatExpiry(newLease.getExpiry()));
              reloadView();
              config.getScavengeWatcher().onAssign(resource, nextCandidate);
            }
          }
        }
      }
    }
  }
  
  private void reloadView() {
    synchronized (viewLock) {
      final LeaseViewImpl newLeaseView = new LeaseViewImpl(nextViewVersion++);
      for (Map.Entry<String, byte[]> leaseTableEntry : leases.entrySet()) {
        final Lease lease = Lease.unpack(leaseTableEntry.getValue());
        newLeaseView.put(leaseTableEntry.getKey(), lease);
      }
      leaseView = newLeaseView;
    }
  }

  public LeaseView getLeaseView() {
    return leaseView;
  }
  
  public void extend(String resource, UUID tenant) throws NotTenantException {
    for (;;) {
      final Lease existingLease = checkCurrent(resource, tenant);
      final Lease newLease = new Lease(tenant, System.currentTimeMillis() + config.getLeaseDuration());
      final boolean extended = leases.replace(resource, existingLease.pack(), newLease.pack());
      if (extended) {
        reloadView();
        return;
      } else {
        reloadView();
      }
    }
  }
  
  public void yield(String resource, UUID tenant) throws NotTenantException {
    for (;;) {
      final Lease existingLease = checkCurrent(resource, tenant);
      final boolean removed = leases.remove(resource, existingLease.pack());
      if (removed) {
        reloadView();
        return;
      } else {
        reloadView();
      }
    }
  }
  
  private Lease checkCurrent(String resource, UUID assumedTenant) throws NotTenantException {
    final Lease existingLease = leaseView.getOrDefault(resource, Lease.vacant());
    if (! existingLease.isHeldByAndCurrent(assumedTenant)) {
      final String m = String.format("Leader of %s is %s until %s", 
                                     resource, existingLease.getTenant(), Lease.formatExpiry(existingLease.getExpiry()));
      throw new NotTenantException(m);
    } else {
      return existingLease;
    }
  }
  
  @Override
  public Joinable terminate() {
    scavengerThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return scavengerThread.join(timeoutMillis);
  }
}
