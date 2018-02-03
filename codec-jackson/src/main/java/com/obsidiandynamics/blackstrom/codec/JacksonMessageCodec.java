package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.function.*;

import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class JacksonMessageCodec implements MessageCodec {  
  @FunctionalInterface
  public interface JacksonExpansion extends Consumer<SimpleModule> {}

  private final ObjectMapper mapper;
  
  public JacksonMessageCodec(boolean mapPayload, JacksonExpansion... expansions) {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    
    final SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new JacksonMessageSerializer());
    module.addDeserializer(Message.class, new JacksonMessageDeserializer(mapPayload));
    module.addSerializer(Payload.class, new JacksonPayloadSerializer());
    module.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    for (JacksonExpansion expansion : expansions) expansion.accept(module);
    
    mapper.registerModule(module);
  }
  
  @Override
  public byte[] encode(Message message) throws JsonProcessingException {
    return mapper.writeValueAsBytes(message);
  }

  @Override
  public Message decode(byte[] bytes) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(bytes, Message.class);
  }
}