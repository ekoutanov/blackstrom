package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractRandomBankTransferTest extends BaseBankTest {
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  private static final boolean LOG_BENCHMARK = false;
  
  @Test
  public final void testRandomTransfersAutonomous() {
    final int branches = Testmark.isEnabled() ? 2 : 10;
    testRandomTransfers(branches, 100 * SCALE, true, true, true, true);
  }
  
  @Test
  public final void testRandomTransfersCoordinated() {
    final int branches = Testmark.isEnabled() ? 2 : 10;
    testRandomTransfers(branches, 100 * SCALE, true, true, true, false);
  }

  @Test
  public final void testRandomTransfersAutonomousBenchmark() {
    Testmark.ifEnabled("autonomous", () -> testRandomTransfers(2, 4_000_000 * SCALE, false, LOG_BENCHMARK, false, true));
  }

  @Test
  public final void testRandomTransfersCoordinatedBenchmark() {
    Testmark.ifEnabled("coordinated", () -> testRandomTransfers(2, 4_000_000 * SCALE, false, LOG_BENCHMARK, false, false));
  }

  private void testRandomTransfers(int numBranches, int runs, boolean randomiseRuns, boolean loggingEnabled, 
                                   boolean trackingEnabled, boolean autonomous) {
    final long transferAmount = 1_000;
    final long initialBalance = runs * transferAmount / (numBranches * numBranches);
    final int backlogTarget = Math.min(runs / 10, 10_000);
    final boolean idempotencyEnabled = false;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final AtomicInteger timeouts = new AtomicInteger();
    
    final long started = System.currentTimeMillis();
    final AtomicBoolean progressLoggerThreadRunning = new AtomicBoolean(true);
    final Thread progressLoggerThread = new Thread(() -> {
      while (progressLoggerThreadRunning.get()) {
        TestSupport.sleep(2000);
        final int c = commits.get(), a = aborts.get(), t = timeouts.get(), s = c + a + t;
        final long took = System.currentTimeMillis() - started;
        final double rate = 1000d * s / took;
        System.out.format("%,d commits | %,d aborts | %,d timeouts | %,d total [%,.0f/s]\n", 
                          c, a, t, s, rate);
      }
    }, AbstractRandomBankTransferTest.class.getSimpleName() + "-progress");
    progressLoggerThread.setDaemon(true);
    progressLoggerThread.start();
    
    final Sandbox sandbox = Sandbox.forInstance(this);
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        (o.getResolution() == Resolution.COMMIT ? commits : o.getAbortReason() == AbortReason.REJECT ? aborts : timeouts)
        .incrementAndGet();
      }
    };
    final BankBranch[] branches = BankBranch.create(numBranches, initialBalance, idempotencyEnabled, sandbox);
    if (autonomous) {
      buildAutonomousManifold(new MonitorEngineConfig().withTrackingEnabled(trackingEnabled),
                              initiator, 
                              branches);
    } else {
      buildCoordinatedManifold(new MonitorEngineConfig().withTrackingEnabled(trackingEnabled),
                               initiator, 
                               branches);
    }

    final long took = TestSupport.took(() -> {
      String[] branchIds = null;
      BankSettlement settlement = null;
      if (! randomiseRuns) {
        branchIds = numBranches != 2 ? BankBranch.generateIds(numBranches) : TWO_BRANCH_IDS;
        settlement = BankSettlement.randomise(branchIds, transferAmount);
      }
      
      final long ballotIdBase = System.currentTimeMillis() << 32;
      for (int run = 0; run < runs; run++) {
        if (randomiseRuns) {
          branchIds = numBranches != 2 ? BankBranch.generateIds(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
          settlement = BankSettlement.randomise(branchIds, transferAmount);
        }
        final Proposal p = new Proposal(Long.toHexString(ballotIdBase + run), branchIds, settlement, PROPOSAL_TIMEOUT)
            .withShardKey(sandbox.key());
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("proposing %s\n", p);
        ledger.append(p);

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - getMinOutcomes(branches));
            if (backlog >= backlogTarget) {
              TestSupport.sleep(1);
              if (loggingEnabled && System.currentTimeMillis() - lastLogTime > 5_000) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d txns)\n", backlog, run);
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }
      progressLoggerThreadRunning.set(false);
      
      wait.until(() -> {
        assertEquals(runs, commits.get() + aborts.get() + timeouts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
        assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
        assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts | %,d timeouts)\n", 
                      runs, took, (double) runs / took * 1000, commits.get(), aborts.get(), timeouts.get());
  }
  
  private long getMinOutcomes(BankBranch[] branches) {
    long minOutcomes = Long.MAX_VALUE;
    for (BankBranch branch : branches) {
      final long outcomes = branch.getNumOutcomes();
      if (outcomes < minOutcomes) {
        minOutcomes = outcomes;
      }
    }
    return minOutcomes;
  }
}