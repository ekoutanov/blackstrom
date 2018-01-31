package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.kafka.*;

public final class MockKafkaLedger {
  private MockKafkaLedger() {}
  
  public static KafkaLedger create() {
    return new KafkaLedger(new MockKafka<>(), "mock", false);
  }
}