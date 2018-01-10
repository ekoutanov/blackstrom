package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaKryoMessageDeserializer implements Deserializer<Message> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaKryoMessageDeserializer.class);
  
  static final class MessageDeserializationException extends KafkaException {
    private static final long serialVersionUID = 1L;

    MessageDeserializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private MessageCodec codec;
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    codec = new KryoMessageCodec();
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    try {
      return codec.decode(data);
    } catch (Throwable e) {
      LOG.error("Error deserializing message " + data, e);
      throw new MessageDeserializationException("Error deserializing message", e);
    }
  }

  @Override
  public void close() {}
}
