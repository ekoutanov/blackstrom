package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.function.*;

import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class JacksonMessageCodec implements MessageCodec {  
  public static final int ENCODING_VERSION = 2;
  
  private static final JacksonExpansion[] defExpansions = { new JacksonDefaultOutcomeMetadataExpansion() };
  
  @FunctionalInterface
  public interface JacksonExpansion extends Consumer<SimpleModule> {}

  private final ObjectMapper mapper;
  
  public JacksonMessageCodec(@YInject(name="mapPayload") boolean mapPayload, 
                             @YInject(name="expansions") JacksonExpansion... expansions) {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    
    final SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new JacksonMessageSerializer());
    module.addDeserializer(Message.class, new JacksonMessageDeserializer(mapPayload));
    module.addSerializer(Payload.class, new JacksonPayloadSerializer());
    module.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    
    for (JacksonExpansion expansion : defExpansions) expansion.accept(module);
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
