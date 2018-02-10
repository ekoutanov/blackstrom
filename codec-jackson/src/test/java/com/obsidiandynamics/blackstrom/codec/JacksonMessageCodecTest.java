package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.util.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.fasterxml.jackson.databind.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.codec.JacksonMessageDeserializer.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.yconf.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class JacksonMessageCodecTest implements TestSupport {
  private static void logEncoded(String encoded) {
    if (LOG) LOG_STREAM.format("encoded %s\n", encoded);
  }
  
  private static void logReencoded(String reencoded) {
    if (LOG) LOG_STREAM.format("re-encoded %s\n", reencoded);
  }
  
  private static void logDecoded(Message m, Object p) {
    if (LOG) LOG_STREAM.format("decoded %s (type=%s)\n", m, (p != null ? p.getClass().getSimpleName() : "n/a"));
  }
  
  @Rule 
  public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void testProposalNullObjective() throws Exception {
    final Message m = new Proposal("N100", new String[] {"a", "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);

    final Proposal d1 = (Proposal) c.decodeText(encoded);
    logDecoded(d1, d1.getObjective());
    assertEquals(m, d1);
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);

    c = new JacksonMessageCodec(true);
    final Proposal d2 = (Proposal) c.decodeText(reencoded);
    logDecoded(d2, d2.getObjective());
    assertEquals(m, d2);
  }
  
  @Test
  public void testCycle() throws Exception {
    testCycle(100);
  }
  
  @Test
  public void testCycleBenchmark() throws Exception {
    Testmark.ifEnabled(() -> testCycle(10_000_000));
  }
  
  private static void testCycle(int runs) throws Exception {
    final MessageCodec c = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(JacksonMessageCodec.class.getClassLoader().getResourceAsStream("jackson.conf"))
        .map(MessageCodec.class);
    cycle(runs, 
          c, 
          new Proposal("N100", new String[] {"a", "b"}, null, 1000), 
          "hollow");
    cycle(runs, 
          c, 
          new Proposal("N100", new String[] {"a", "b"}, 
                       new JacksonDog().named("Rex").withFriend(new JacksonCat().named("Tigger")), 
                       1000), 
          "animal");
    cycle(runs, 
          c, 
          new Proposal("N100", new String[] {"a", "b"}, BankSettlement.forTwo(1000), 1000), 
          "branch");
  }
  
  private static void cycle(int runs, MessageCodec c, Message m, String name) throws Exception {
    final long tookSer = TestSupport.tookThrowing(() -> {
      for (int i = 0; i < runs; i++) {
        final byte[] encoded = c.encode(m);
        if (encoded == null) throw new AssertionError();
      }
    });
    System.out.format("%s ser'n: %,d took %,d ms, %,.0f msgs/sec\n", name, runs, tookSer, (double) runs / tookSer * 1000);
    
    final long tookDes = TestSupport.tookThrowing(() -> {
      final byte[] encoded = c.encode(m);
      for (int i = 0; i < runs; i++) {
        final Message d = c.decode(encoded);
        if (d == null) throw new AssertionError();
      }
    });
    System.out.format("%s des'n: %,d took %,d ms, %,.0f msgs/sec\n", name, runs, tookDes, (double) runs / tookDes * 1000);
  }
  
  @Test
  public void testProposalNonNullObjective() throws Exception {
    final JacksonAnimal<?> a = new JacksonDog().named("Rover").withFriend(new JacksonCat().named("Misty"));
    final Proposal m = new Proposal("N100", new String[] {"a", "b"}, a, 1000);
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Proposal d1 = (Proposal) c.decodeText(encoded);
    logDecoded(d1, d1.getObjective());
    assertNotNull(d1.getObjective());
    assertEquals(LinkedHashMap.class, d1.getObjective().getClass());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Proposal d2 = (Proposal) c.decodeText(reencoded);
    logDecoded(d2, d2.getObjective());
    assertEquals(m, d2);
  }

  @Test
  public void testVoteNonNullMetadata() throws Exception {
    final JacksonAnimal<?> a = new JacksonDog().named("Rex").withFriend(new JacksonCat().named("Tigger"));
    final Vote m = new Vote("V100", new Response("test-cohort", Intent.ACCEPT, a)).withSource("test");
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Vote d1 = (Vote) c.decodeText(encoded);
    logDecoded(d1, d1.getResponse().getMetadata());
    assertNotNull(d1.getResponse().getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponse().getMetadata().getClass());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Vote d2 = (Vote) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponse().getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeCommitMixedMetadata() throws Exception {
    final JacksonAnimal<?> a = new JacksonDog().named("Rex").withFriend(new JacksonCat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Intent.ACCEPT, a);
    final Response rb = new Response("test-cohort-b", Intent.ACCEPT, null);
    final Outcome m = new Outcome("O100", Resolution.COMMIT, null, new Response[] {ra, rb}).withSource("test");
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decodeText(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Outcome d2 = (Outcome) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeAbortMixedMetadata() throws Exception {
    final JacksonAnimal<?> a = new JacksonDog().named("Rex").withFriend(new JacksonCat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Intent.REJECT, a);
    final Response rb = new Response("test-cohort-b", Intent.ACCEPT, null);
    final Outcome m = new Outcome("O100", Resolution.ABORT, AbortReason.REJECT, new Response[] {ra, rb});
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decodeText(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Outcome d2 = (Outcome) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }
  
  @Test
  public void testUnknownSerialize() throws Exception {
    final UnknownMessage m = new UnknownMessage("U400");
    final MessageCodec c = new JacksonMessageCodec(false);
    thrown.expect(JsonMappingException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    c.encode(m);
  }
  
  @Test
  public void testUnknownDeserialize() throws Exception {
    final MessageCodec c = new JacksonMessageCodec(false);
    final String encoded = "{\"messageType\":\"$UNKNOWN\",\"ballotId\":\"$U400\",\"timestamp\":1000}";
    thrown.expect(MessageDeserializationException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    c.decodeText(encoded);
  }
  
  @Test
  public void testExpansion() throws Exception {
    final Message m = new Proposal("N100", new String[] {"a", "b"}, BankSettlement.forTwo(1000), 1000).withSource("test");
    final MessageCodec c = new JacksonMessageCodec(true, new JacksonBankExpansion());
    
    final String encoded = c.encodeText(m);
    logEncoded(encoded);

    final Proposal d = (Proposal) c.decodeText(encoded);
    logDecoded(d, d.getObjective());
    assertEquals(m, d);
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(JacksonMessageCodecTest.class);
  }
}
