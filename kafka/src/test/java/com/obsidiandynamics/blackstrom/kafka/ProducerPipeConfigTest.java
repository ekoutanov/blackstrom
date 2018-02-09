package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class ProducerPipeConfigTest {
  @Test
  public void testConfig() throws IOException {
    final ProducerPipeConfig config = new MappingContext()
    .withParser(new SnakeyamlParser())
    .fromStream(ProducerPipeConfigTest.class.getClassLoader().getResourceAsStream("producerpipe-config.yaml"))
    .map(ProducerPipeConfig.class);
    
    assertEquals(true, config.isAsync());
    Assertions.assertToStringOverride(config);
  }
}
