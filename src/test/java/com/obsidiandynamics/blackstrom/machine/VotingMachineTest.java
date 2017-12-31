package com.obsidiandynamics.blackstrom.machine;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class VotingMachineTest {
  @Test
  public void testBuilder() {
    final Ledger ledger = mock(Ledger.class);
    final Initiator initiator = mock(Initiator.class);
    final Cohort cohort = mock(Cohort.class);
    final Monitor monitor = mock(Monitor.class);
    
    final VotingMachine machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(initiator, cohort, monitor)
        .build();
    
    verify(initiator).init(notNull());
    verify(cohort).init(notNull());
    verify(monitor).init(notNull());
    verify(ledger).init();
    
    assertEquals(ledger, machine.getLedger());
    assertTrue(machine.getHandlers().contains(initiator));
    assertTrue(machine.getHandlers().contains(cohort));
    assertTrue(machine.getHandlers().contains(monitor));
    
    machine.dispose();
    
    verify(ledger).dispose();
    verify(initiator).dispose();
    verify(cohort).dispose();
    verify(monitor).dispose();
  }
}
