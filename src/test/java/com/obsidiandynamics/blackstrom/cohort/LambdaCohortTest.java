package com.obsidiandynamics.blackstrom.cohort;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohortTest {
  @Test
  public void testDefaultInitAndDispose() {
    final LambdaCohort l = LambdaCohort.builder()
        .build();
    l.init(null);
    l.dispose();
  }
  
  @Test
  public void testGroupId() {
    final LambdaCohort l = LambdaCohort.builder()
        .withGroupId("test")
        .build();
    
    assertEquals("test", l.getGroupId());
  }
  
  @Test
  public void testDefaultHandlers() {
    final InitContext initContext = mock(InitContext.class);
    final MessageContext messageContext = mock(MessageContext.class);
    final LambdaCohort l = LambdaCohort.builder().build();
    final Query query = new Query("X0", null, 1_000);
    final Command command = new Command("X0", null, 1_000);
    final Notice notice = new Notice("X0", null);
    final Proposal proposal = new Proposal("X0", new String[0], null, 1_000);
    final Vote vote = new Vote("X0", null);
    final Outcome outcome = new Outcome("X0", Resolution.COMMIT, null, new Response[0], null);
    
    l.init(initContext);
    l.onQuery(messageContext, query);
    l.onCommand(messageContext, command);
    l.onNotice(messageContext, notice);
    l.onProposal(messageContext, proposal);
    l.onVote(messageContext, vote);
    l.onOutcome(messageContext, outcome);
    l.dispose();
    
    verifyNoMoreInteractions(initContext, messageContext);
  }
  
  @Test
  public void testHandlers() {
    final Initable onInit = mock(Initable.class);
    final Disposable onDispose = mock(Disposable.class);
    final QueryProcessor onQuery = mock(QueryProcessor.class);
    final CommandProcessor onCommand = mock(CommandProcessor.class);
    final NoticeProcessor onNotice = mock(NoticeProcessor.class);
    final ProposalProcessor onProposal = mock(ProposalProcessor.class);
    final VoteProcessor onVote = mock(VoteProcessor.class);
    final OutcomeProcessor onOutcome = mock(OutcomeProcessor.class);
    
    final InitContext initContext = mock(InitContext.class);
    final MessageContext messageContext = mock(MessageContext.class);
    final Query query = new Query("X0", null, 1_000);
    final Command command = new Command("X0", null, 1_000);
    final Notice notice = new Notice("X0", null);
    final Proposal proposal = new Proposal("X0", new String[0], null, 1_000);
    final Vote vote = new Vote("X0", null);
    final Outcome outcome = new Outcome("X0", Resolution.COMMIT, null, new Response[0], null);
    
    final LambdaCohort l = LambdaCohort.builder()
        .onInit(onInit)
        .onDispose(onDispose)
        .onQuery(onQuery)
        .onCommand(onCommand)
        .onNotice(onNotice)
        .onProposal(onProposal)
        .onVote(onVote)
        .onOutcome(onOutcome)
        .build();
    
    assertNull(l.getGroupId());
    
    l.init(initContext);
    verify(onInit).init(eq(initContext));
    
    l.onQuery(messageContext, query);
    verify(onQuery).onQuery(eq(messageContext), eq(query));
    
    l.onCommand(messageContext, command);
    verify(onCommand).onCommand(eq(messageContext), eq(command));
    
    l.onNotice(messageContext, notice);
    verify(onNotice).onNotice(eq(messageContext), eq(notice));
    
    l.onProposal(messageContext, proposal);
    verify(onProposal).onProposal(eq(messageContext), eq(proposal));
    
    l.onVote(messageContext, vote);
    verify(onVote).onVote(eq(messageContext), eq(vote));
    
    l.onOutcome(messageContext, outcome);
    verify(onOutcome).onOutcome(eq(messageContext), eq(outcome));
    
    l.dispose();
    verify(onDispose).dispose();
    
    verifyNoMoreInteractions(initContext, messageContext, onInit, onDispose, onQuery, onCommand, onNotice, onProposal, onVote, onOutcome);
  }
}
