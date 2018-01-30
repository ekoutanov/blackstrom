package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractLedgerTest implements TestSupport {
  private static final String[] TEST_COHORTS = new String[] {"a", "b"};
  
  private class TestHandler implements MessageHandler, Groupable.NullGroup {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(MessageContext context, Message message) {
      if (! shard.contains(message)) return;
      
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
      context.confirm(message.getMessageId());
    }
  }
  
  private Ledger ledger;
  
  private long messageId;
  
  private final Shard shard = Shard.forTest(this);
  
  private final Timesert wait = getWait();
  
  protected abstract Timesert getWait();
  
  protected abstract Ledger createLedger();
  
  @After
  public void afterBase() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  @Test
  public final void testPubSub() {
    ledger = createLedger();
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
      appendMessage("test");
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
  
  protected static void enableBenchmark() {
    System.setProperty(AbstractLedgerTest.class.getSimpleName() + ".benchmark", String.valueOf(true));
  }
  
  @Test
  public final void testOneWay() {
    testOneWay(10_000);
  }
  
  @Test
  public final void testOneWayBenchmark() {
    if (PropertyUtils.get(AbstractLedgerTest.class.getSimpleName() + ".benchmark", Boolean::parseBoolean, false)) {
      testOneWay(10_000_000);
    }
  }
  
  private final void testOneWay(int numMessages) {
    ledger = createLedger();
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (shard.contains(m)) {
        received.incrementAndGet();
      }
    });
    ledger.init();
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("test");
      }
      wait.until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("One-way: %,d took %,d ms, %,.0f msgs/sec\n", 
                      numMessages, took, (float) numMessages / took * 1000);
  }
  
  @Test
  public final void testTwoWay() {
    testTwoWay(10_000);
  }
  
  @Test
  public final void testTwoWayBenchmark() {
    if (PropertyUtils.get(AbstractLedgerTest.class.getSimpleName() + ".benchmark", Boolean::parseBoolean, false)) {
      testTwoWay(10_000_000);
    }
  }
  
  private final void testTwoWay(int numMessages) {
    ledger = createLedger();
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (shard.contains(m) && m.getSource().equals("source")) {
        c.getLedger().append(new Proposal(m.getBallotId(), 0, TEST_COHORTS, null, 0).withSource("echo"));
        c.confirm(m.getMessageId());
      }
    });
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getSource().equals("echo")) {
        received.incrementAndGet();
        c.confirm(m.getMessageId());
      }
    });
    ledger.init();
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("source");
      }
      wait.until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("Two-way: %,d took %,d ms, %,d msgs/sec\n", numMessages, took, numMessages / took * 1000);
  }
  
  private void appendMessage(String source) {
    ledger.append(new Proposal(String.valueOf(messageId++), 0, TEST_COHORTS, null, 0)
                  .withSource(source)
                  .withShardKey(shard.key()));
  }
}
