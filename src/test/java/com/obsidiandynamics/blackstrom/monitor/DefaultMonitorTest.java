package com.obsidiandynamics.blackstrom.monitor;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class DefaultMonitorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private final InitContext initContext = new DefaultInitContext(new Ledger() {
    @Override public void attach(MessageHandler handler) {
      ledger.attach(handler);
    }

    @Override public void append(Message message, AppendCallback callback) {
      ledger.append(message, callback);
    }
    
    @Override public void confirm(Object handlerId, Object messageId) {
      ledger.confirm(handlerId, messageId);
    }
    
    @Override public void init() {
      ledger.init();
    }
    
    @Override public void dispose() {
      ledger.dispose();
    }
  });
  
  private DefaultMonitor monitor;
  
  private MessageContext context;
  
  private Ledger ledger;
  
  private final List<Vote> votes = new CopyOnWriteArrayList<>();
  
  private final List<Outcome> outcomes = new CopyOnWriteArrayList<>();
  
  @Before
  public void before() {
    setMonitorAndInit(new DefaultMonitor());
    setLedger(new SingleNodeQueueLedger());
    ledger.attach((NullGroupMessageHandler) (c, m) -> { 
      (m.getMessageType() == MessageType.OUTCOME ? outcomes : votes).add(Cast.from(m));
    });
    ledger.init();
    context = new DefaultMessageContext(ledger, null);
  }
  
  @After
  public void after() {
    monitor.dispose();
    ledger.dispose();
  }
  
  private void setMonitorAndInit(DefaultMonitor monitor) {
    if (this.monitor != null) this.monitor.dispose();
    this.monitor = monitor;
    monitor.init(initContext);
  }
  
  private void setLedger(Ledger ledger) {
    if (this.ledger != null) this.ledger.dispose();
    this.ledger = ledger;
  }
  
  @Test
  public void testInitialisation() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withGroupId("test-monitor")));
    assertEquals("test-monitor", monitor.getGroupId());
  }
  
  @Test
  public void testProposalOutcome_oneCohort() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals("ACCEPT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals("REJECT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
  }
  
  @Test
  public void testProposalOutcome_twoCohorts() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Intent.REJECT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    outcomes.clear();
  }
  
  @Test
  public void testDuplicateProposal_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    nominate(ballotId, "a", "b", "c");
    vote(ballotId, "a", Intent.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    nominate(ballotId, "a", "b", "c");

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    nominate(ballotId, "a", "b", "c");
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testDuplicateVote_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    vote(ballotId, "a", Intent.REJECT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    vote(ballotId, "b", Intent.TIMEOUT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    vote(ballotId, "b", Intent.REJECT);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testVoteWithoutBallot() {
    final UUID ballotId = UUID.randomUUID();
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testGCNoReap() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions()
                                       .withOutcomeLifetime(60_000)
                                       .withGCInterval(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    
    TestSupport.sleep(10);
    assertEquals(1, monitor.getOutcomes().size());
  }
  
  @Test
  public void testGCReap() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withOutcomeLifetime(1).withGCInterval(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    
    wait.untilTrue(() -> monitor.getOutcomes().isEmpty());
  }
  
  private static class TestLedgerException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  @Test
  public void testAppendError() {
    setLedger(Mockito.mock(Ledger.class));
    Mockito.doThrow(TestLedgerException.class).when(ledger).append(Mockito.any());
    ledger.attach((NullGroupMessageHandler) (c, m) -> outcomes.add((Outcome) m));
    ledger.init();
    context = new DefaultMessageContext(ledger, null);
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testExplicitTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1)));
    
    final UUID ballotId = UUID.randomUUID();
    final long startTimestamp = System.currentTimeMillis();
    nominate(ballotId, 0, "a", "b");
    vote(ballotId, startTimestamp, "a", Intent.ACCEPT);
    
    wait.until(numVotesIsAtLeast(1));
    assertEquals(0, outcomes.size());
    assertEquals(ballotId, votes.get(0).getBallotId());
    assertEquals(Intent.TIMEOUT, votes.get(0).getResponse().getIntent());
    
    // feed the timeout back into the monitor - should produce a rejection
    vote(ballotId, "b", Intent.TIMEOUT);
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.EXPLICIT_TIMEOUT, outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.TIMEOUT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    
    // subsequent votes should have no effect
    vote(ballotId, "b", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(1, outcomes.size());
  }
  
  @Test
  public void testNoTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1)));
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, 10_000, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    assertEquals(0, votes.size());
    
    vote(ballotId, "b", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
  }
  
  @Test
  public void testImplicitTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(60_000)));
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, 1, "a", "b");
    vote(ballotId, System.currentTimeMillis() + 1_000, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(AbortReason.IMPLICIT_TIMEOUT, outcomes.get(0).getAbortReason());
  }
  
  @Test
  public void testTimeoutVoteBadLedger() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1)));
    final Ledger ledger = mock(Ledger.class);
    setLedger(ledger);
    ledger.init();
    
    final AtomicBoolean responded = new AtomicBoolean();
    doAnswer(invocation -> {
      final AppendCallback callback = invocation.getArgument(1);
      callback.onAppend(null, new Exception("simulated append error"));
      responded.set(true);
      return null;
    }).when(ledger).append(any(), any());
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, 0, "a");
    
    wait.untilTrue(responded::get);
  }
  
  @Test
  public void testOutcomeBadLedger() {
    final Ledger ledger = mock(Ledger.class);
    setLedger(ledger);
    ledger.init();
    
    final AtomicBoolean responded = new AtomicBoolean();
    doAnswer(invocation -> {
      final AppendCallback callback = invocation.getArgument(1);
      callback.onAppend(null, new Exception("simulated append error"));
      responded.set(true);
      return null;
    }).when(ledger).append(any(), any());
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.untilTrue(responded::get);
  }
  
  private Runnable numVotesIsAtLeast(int size) {
    return () -> assertTrue("votes.size=" + votes.size(), votes.size() >= size);
  }
  
  private Runnable numOutcomesIs(int size) {
    return () -> assertEquals(size, outcomes.size());
  }
  
  private Response getResponseForCohort(Outcome outcome, String cohort) {
    return Arrays.stream(outcome.getResponses()).filter(r -> r.getCohort().equals(cohort)).findAny().get();
  }

  private void nominate(UUID ballotId, String... cohorts) {
    nominate(ballotId, Integer.MAX_VALUE, cohorts);
  }

  private void nominate(UUID ballotId, int ttl, String... cohorts) {
    monitor.onProposal(context, new Proposal(ballotId, cohorts, null, ttl));
  }

  private void vote(UUID ballotId, String cohort, Intent intent) {
    vote(ballotId, 0, cohort, intent);
  }

  private void vote(UUID ballotId, long timestamp, String cohort, Intent intent) {
    monitor.onVote(context, new Vote(ballotId, timestamp, new Response(cohort, intent, intent.name())));
  }
}
