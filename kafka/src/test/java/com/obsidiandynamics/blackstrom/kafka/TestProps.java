package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.yconf.props.*;

final class TestProps {
  private TestProps() {}
  
  static Properties producer() {
    return new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .build();
  }
  
  static Properties consumer() {
    return new PropsBuilder()
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaMessageDeserializer.class.getName())
        .build();
  }
}
