package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BankTransferTest {  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final String[] TWO_BRANCH_IDS = new String[] { getBranchId(0), getBranchId(1) };
  private static final int TWO_BRANCHES = TWO_BRANCH_IDS.length;
  private static final int FUTURE_GET_TIMEOUT = 10_000;
  
  private final Timesert wait = Wait.SHORT;

  private Ledger ledger;

  private VotingMachine machine;

  @After
  public void after() {
    machine.dispose();
  }
  
  private Ledger createLedger() {
    return new MultiNodeQueueLedger();
  }
  
  private void buildStandardMachine(Factor initiator, Factor monitor, Factor... branches) {
    ledger = createLedger();
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(initiator, monitor)
        .withFactors(branches)
        .build();
  }

  @Test
  public void testCommit() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS,
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.COMMIT, o.getVerdict());
    assertEquals(2, o.getResponses().length);
    assertEquals(Pledge.ACCEPT, o.getResponse(getBranchId(0)).getPledge());
    assertEquals(Pledge.ACCEPT, o.getResponse(getBranchId(1)).getPledge());
    wait.until(() -> {
      assertEquals(initialBalance - transferAmount, branches[0].getBalance());
      assertEquals(initialBalance + transferAmount, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue(allZeroEscrow(branches));
    });
  }

  @Test
  public void testAbort() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance + 1;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1); // the accept status doesn't need to have been considered
    assertEquals(Pledge.REJECT, o.getResponse(getBranchId(0)).getPledge());
    final Response acceptResponse = o.getResponse(getBranchId(1));
    if (acceptResponse != null) {
      assertEquals(Pledge.ACCEPT, acceptResponse.getPledge());  
    }
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue(allZeroEscrow(branches));
    });
  }

  @Test
  public void testImplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, 
                         monitor, 
                         branches[0], 
                         new FallibleFactor(branches[1]).withRxFailureMode(new DelayedDelivery(1, 10)));

    final Outcome o = initiator.initiate(UUID.randomUUID(),
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         1)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1);
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue(allZeroEscrow(branches));
    });
  }

  @Test
  public void testExplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1));
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, 
                         monitor, 
                         branches[0], 
                         new FallibleFactor(branches[1]).withTxFailureMode(new DelayedDelivery(1, 60_000)));

    final Outcome o = initiator.initiate(UUID.randomUUID(),
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         1)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue(allZeroEscrow(branches));
    });
  }
  
  @Test
  public void testFactorFailures() {
    final RxTxFailureModes[] presetFailureModesArray = new RxTxFailureModes[] {
      new RxTxFailureModes() {},
      new RxTxFailureModes() {{
        rxFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDuplicateDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDuplicateDelivery(1, 10);
      }}
    };
    
    for (TargetFactor target : TargetFactor.values()) {
      for (RxTxFailureModes failureModes : presetFailureModesArray) {
        try {
          testFactorFailure(new FailureModes().set(target, failureModes));
        } catch (Exception e) {
          throw new AssertionError(String.format("target=%s, failureModes=%s", target, failureModes), e);
        }
        machine.dispose();
      }
    }
  }
  
  private enum TargetFactor {
    INITIATOR,
    COHORT,
    MONITOR
  }
  
  private abstract static class RxTxFailureModes {
    FailureMode rxFailureMode; 
    FailureMode txFailureMode;
    
    @Override
    public String toString() {
      return RxTxFailureModes.class.getSimpleName() + " [rxFailureMode=" + rxFailureMode + 
          ", txFailureMode=" + txFailureMode + "]";
    }
  }
  
  private static class FailureModes extends EnumMap<TargetFactor, RxTxFailureModes> {
    private static final long serialVersionUID = 1L;
    
    FailureModes() {
      super(TargetFactor.class);
      for (TargetFactor target : TargetFactor.values()) {
        set(target, new RxTxFailureModes() {});
      }
    }
    
    FailureModes set(TargetFactor target, RxTxFailureModes rxtxFailureModes) {
      put(target, rxtxFailureModes);
      return this;
    }
  }

  private void testFactorFailure(Map<TargetFactor, RxTxFailureModes> failureModes) throws InterruptedException, ExecutionException, Exception {
    final int initialBalance = 1_000;
    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);

    ledger = createLedger();
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(new FallibleFactor(initiator)
                     .withRxFailureMode(failureModes.get(TargetFactor.INITIATOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.INITIATOR).txFailureMode),
                     new FallibleFactor(monitor)
                     .withRxFailureMode(failureModes.get(TargetFactor.MONITOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.MONITOR).txFailureMode),
                     new FallibleFactor(branches[0])
                     .withRxFailureMode(failureModes.get(TargetFactor.COHORT).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.COHORT).txFailureMode),
                     branches[1])
        .build();

    testSingleTransfer(initialBalance + 1, Verdict.ABORT, initiator);
    testSingleTransfer(initialBalance, Verdict.COMMIT, initiator);

    wait.until(() -> {
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue(allZeroEscrow(branches));
    });
  }

  private void testSingleTransfer(int transferAmount, Verdict expectedVerdict,
                                  AsyncInitiator initiator) throws InterruptedException, ExecutionException, Exception {
    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(expectedVerdict, o.getVerdict());
  }

  @Test
  public void testRandomTransfersBenchmark() throws Exception {
    final int numBranches = 10;
    final long initialBalance = 1_000_000;
    final long transferAmount = 1_000;
    final int runs = 10_000;
    final int backlogTarget = 10_000;
    final boolean idempotencyEnabled = false;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (o.getVerdict() == Verdict.COMMIT) {
        commits.incrementAndGet();
      } else {
        aborts.incrementAndGet();
      }
    };
    final BankBranch[] branches = createBranches(numBranches, initialBalance, idempotencyEnabled);
    final Monitor monitor = new DefaultMonitor();
    buildStandardMachine(initiator, monitor, branches);

    final long took = TestSupport.tookThrowing(() -> {
      for (int run = 0; run < runs; run++) {
        final String[] branchIds = numBranches != TWO_BRANCHES ? generateRandomBranches(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
        final BankSettlement settlement = generateRandomSettlement(branchIds, transferAmount);
        ledger.append(new Nomination(run, branchIds, settlement, Integer.MAX_VALUE));

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = run - commits.get() - aborts.get();
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (System.currentTimeMillis() - lastLogTime > 5_000) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d txns)\n", backlog, run);
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }

      wait.until(() -> {
        assertEquals(runs, commits.get() + aborts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
        assertTrue(allZeroEscrow(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts)\n", 
                      runs, took, (double) runs / took * 1000, commits.get(), aborts.get());
  }

  private static long getTotalBalance(BankBranch[] branches) {
    return Arrays.stream(branches).collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
  }

  private static boolean allZeroEscrow(BankBranch[] branches) {
    return Arrays.stream(branches).allMatch(b -> b.getEscrow() == 0);
  }

  private static BankSettlement generateRandomSettlement(String[] branchIds, long amount) {
    final Map<String, BalanceTransfer> transfers = new HashMap<>(branchIds.length);
    long sum = 0;
    for (int i = 0; i < branchIds.length - 1; i++) {
      final long randomAmount = amount - (long) (Math.random() * amount * 2);
      sum += randomAmount;
      final String branchId = branchIds[i];
      transfers.put(branchId, new BalanceTransfer(branchId, randomAmount));
    }
    final String lastBranchId = branchIds[branchIds.length - 1];
    transfers.put(lastBranchId, new BalanceTransfer(lastBranchId, -sum));
    if (TestSupport.LOG) TestSupport.LOG_STREAM.format("xfers %s\n", transfers);
    return new BankSettlement(transfers);
  }

  private static String[] generateRandomBranches(int numBranches) {
    final Set<String> branches = new HashSet<>(numBranches);
    for (int i = 0; i < numBranches; i++) {
      while (! branches.add(getRandomBranchId(numBranches)));
    }
    return branches.toArray(new String[numBranches]);
  }

  private static String getRandomBranchId(int numBranches) {
    return getBranchId((int) (Math.random() * numBranches));
  }

  private static String getBranchId(int branchIdx) {
    return "branch-" + branchIdx;
  }

  private static BankBranch[] createBranches(int numBranches, long initialBalance, boolean idempotencyEnabled) {
    final BankBranch[] branches = new BankBranch[numBranches];
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches[branchIdx] = new BankBranch(getBranchId(branchIdx), initialBalance, idempotencyEnabled);
    }
    return branches;
  }
}
