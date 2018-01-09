package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.yconf.*;

@Y
public final class KafkaCluster<K, V> implements Kafka<K, V> {
  private final KafkaClusterConfig config;
  
  public KafkaCluster(@YInject(name="clusterConfig") KafkaClusterConfig config) {
    config.init();
    this.config = config;
  }
  
  public KafkaClusterConfig getConfig() {
    return config;
  }

  @Override
  public Producer<K, V> getProducer(Properties props) {
    final Properties combinedProps = new Properties();
    combinedProps.putAll(config.getProducerCombinedProps());
    combinedProps.putAll(props);
    return new KafkaProducer<>(combinedProps);
  }

  @Override
  public Consumer<K, V> getConsumer(Properties props) {
    final Properties combinedProps = new Properties();
    combinedProps.putAll(config.getConsumerCombinedProps());
    combinedProps.putAll(props);
    return new KafkaConsumer<>(combinedProps);
  }

  @Override
  public String toString() {
    return KafkaCluster.class.getSimpleName() + " [config: " + config + "]";
  }
}
