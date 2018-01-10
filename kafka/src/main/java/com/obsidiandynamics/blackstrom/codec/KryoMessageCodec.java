package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import org.objenesis.strategy.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.pool.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KryoMessageCodec implements MessageCodec {
  private final KryoPool pool;
  
  public KryoMessageCodec() {
    pool = new KryoPool.Builder(() -> {
          final Kryo kryo = new Kryo();
          kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
          return kryo;
        })
        .softReferences()
        .build();
  }

  @Override
  public byte[] encode(Message message) throws JsonProcessingException {
    final Output output = new Output(256, 65_536);
    final Kryo kryo = pool.borrow();
    try {
      kryo.writeClassAndObject(output, message);
    } finally {
      pool.release(kryo);
    }
    return output.toBytes();
  }

  @Override
  public Message decode(byte[] bytes) throws JsonParseException, JsonMappingException, IOException {
    final Input input = new Input(bytes);
    final Kryo kryo = pool.borrow();
    try {
      return (Message) kryo.readClassAndObject(input);
    } finally {
      pool.release(kryo);
    }
  }
}
