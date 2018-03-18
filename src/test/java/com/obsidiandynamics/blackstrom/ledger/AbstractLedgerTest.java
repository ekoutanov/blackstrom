package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractLedgerTest implements TestSupport {
  private static final String[] TEST_COHORTS = new String[] {"a", "b"};
  private static final Object TEST_OBJECTIVE = BankSettlement.forTwo(1000);

  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  private class TestHandler implements MessageHandler, Groupable.NullGroup {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(MessageContext context, Message message) {
      if (! sandbox.contains(message)) return;
      
      if (LOG) LOG_STREAM.format("Received %s\n", message);
      final long ballotId = Long.parseLong(message.getBallotId());
      if (lastBallotId == -1) {
        lastBallotId = ballotId;
      } else {
        final long expectedBallotId = lastBallotId + 1;
        if (ballotId != expectedBallotId) {
          error = new AssertionError("Expected ballot " + expectedBallotId + ", got " + ballotId);
          throw error;
        } else {
          lastBallotId = ballotId;
        }
      }
      received.add(message);
      context.beginAndConfirm(message);
    }
  }
  
  protected Ledger ledger;
  
  private long messageId;
  
  private final Sandbox sandbox = Sandbox.forInstance(this);
  
  protected final Timesert wait = getWait();
  
  protected abstract Timesert getWait();
  
  protected abstract Ledger createLedger();
  
  @After
  public void afterBase() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  protected void useLedger(Ledger ledger) {
    if (this.ledger != null) this.ledger.dispose();
    this.ledger = ledger;
  }
  
  @Test
  public final void testPubSub() {
    useLedger(createLedger());
    final int numHandlers = 3;
    final int numMessages = 5;
    final List<TestHandler> handlers = new ArrayList<>(numHandlers);
    
    for (int i = 0; i < numHandlers; i++) {
      final TestHandler handler = new TestHandler();
      handlers.add(handler);
      ledger.attach(handler);
    }
    ledger.init();
    
    for (int i = 0; i < numMessages; i++) {
      appendMessage("test", TEST_OBJECTIVE);
    }
    
    boolean success = false;
    try {
      wait.until(() -> {
        for (TestHandler handler : handlers) {
          assertNull(handler.error);
          assertEquals(numMessages, handler.received.size());
          long index = 0;
          for (Message m  : handler.received) {
            assertEquals(String.valueOf(index), m.getBallotId());
            index++;
          }
        }
      });
      success = true;
    } finally {
      if (! success) {
        for (TestHandler handler : handlers) {
          System.out.println("---");
          for (Message m : handler.received) {
            System.out.println("- " + m);
          }
        }
      }
    }
    
    ledger.dispose();
  }
  
  @Test
  public final void testOneWay() {
    testOneWay(2, 4, 10_000 * SCALE);
  }
  
  @Test
  public final void testOneWayBenchmark() {
    Testmark.ifEnabled(() -> {
      testOneWay(1, 1, 2_000_000 * SCALE);
      testOneWay(1, 2, 2_000_000 * SCALE);
      testOneWay(1, 4, 2_000_000 * SCALE);
      testOneWay(2, 4, 1_000_000 * SCALE);
      testOneWay(2, 8, 1_000_000 * SCALE);
      testOneWay(4, 8, 500_000 * SCALE);
      testOneWay(4, 16, 500_000 * SCALE);
    });
  }
  
  private final void testOneWay(int producers, int consumers, int messagesPerProducer) {
    useLedger(createLedger());
    
    final AtomicLong totalSent = new AtomicLong();
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final AtomicLong[] receivedArray = new AtomicLong[consumers];
    for (int i = 0; i < consumers; i++) {
      final AtomicLong received = new AtomicLong();
      receivedArray[i] = received;
      ledger.attach((NullGroupMessageHandler) (c, m) -> {
        if (sandbox.contains(m)) {
          received.incrementAndGet();
        }
      });
    }
    ledger.init();
    
    final LongSupplier totalReceived = () -> {
      long total = 0;
      for (AtomicLong received : receivedArray) {
        total += received.get();
      }
      return total;
    };
    
    final LongSupplier smallestReceived = () -> {
      long smallest = Long.MAX_VALUE;
      for (AtomicLong received : receivedArray) {
        final long r = received.get();
        if (r < smallest) {
          smallest = r;
        }
      }
      return smallest;
    };

    final long took = TestSupport.took(() -> {
      ParallelJob.blocking(producers, threadNo -> {
        for (int i = 0; i < messagesPerProducer; i++) {
          appendMessage("test", TEST_OBJECTIVE);
          
          if (i != 0 && i % checkInterval == 0) {
            final long sent = totalSent.addAndGet(checkInterval);
            while (sent - smallestReceived.getAsLong() >= backlogTarget) {
              TestSupport.sleep(1);
            }
          }
        }
      }).run();

      wait.until(() -> {
        assertEquals(producers * messagesPerProducer * (long) consumers, totalReceived.getAsLong());
      });
    });
                                     
    final long totalMessages = (long) producers * messagesPerProducer * consumers;
    System.out.format("One-way: %d/%d prd/cns, %,d msgs took %,d ms, %,.0f msgs/sec\n", 
                      producers, consumers, totalMessages, took, (double) totalMessages / took * 1000);
  }
  
  @Test
  public final void testTwoWay() {
    testTwoWay(10_000 * SCALE);
  }
  
  @Test
  public final void testTwoWayBenchmark() {
    Testmark.ifEnabled(() -> testTwoWay(2_000_000 * SCALE));
  }
  
  private final void testTwoWay(int numMessages) {
    useLedger(createLedger());

    final AtomicLong totalSent = new AtomicLong();
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (sandbox.contains(m) && m.getSource().equals("source")) {
        c.getLedger().append(new Proposal(m.getBallotId(), 0, TEST_COHORTS, null, 0).withSource("echo"));
        c.beginAndConfirm(m);
      }
    });
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getSource().equals("echo")) {
        received.incrementAndGet();
        c.beginAndConfirm(m);
      }
    });
    ledger.init();
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("source", TEST_OBJECTIVE);
        
        if (i != 0 && i % checkInterval == 0) {
          final long sent = totalSent.addAndGet(checkInterval);
          while (sent - received.get() > backlogTarget) {
            TestSupport.sleep(1);
          }
        }
      }
      wait.until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("Two-way: %,d took %,d ms, %,d msgs/sec\n", numMessages, took, numMessages / took * 1000);
  }
  
  private void appendMessage(String source, Object objective) {
    ledger.append(new Proposal(String.valueOf(messageId++), 0, TEST_COHORTS, objective, 0)
                  .withSource(source)
                  .withShardKey(sandbox.key()));
  }
}
