package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;

final class StreamHelper {
  static final long SMALLEST_OFFSET = 0;
  static final IFunction<byte[], Boolean> NOT_NULL = bytes -> bytes != null;
  
  private StreamHelper() {}
  
  static Ringbuffer<byte[]> getRingbuffer(HazelcastInstance instance, StreamConfig streamConfig) {
    final String streamFQName = QNamespace.HAZELQ_STREAM.qualify(streamConfig.getName());
    final RingbufferConfig ringbufferConfig = new RingbufferConfig(streamFQName)
        .setBackupCount(streamConfig.getSyncReplicas())
        .setAsyncBackupCount(streamConfig.getAsyncReplicas())
        .setCapacity(streamConfig.getHeapCapacity())
        .setRingbufferStoreConfig(new RingbufferStoreConfig()
                                  .setFactoryClassName(getClassName(streamConfig.getStoreFactoryClass()))
                                  .setProperties(new Properties()));
    instance.getConfig().addRingBufferConfig(ringbufferConfig);
    return instance.getRingbuffer(streamFQName);
  }
  
  private static String getClassName(Class<?> storeFactoryClass) {
    return storeFactoryClass != null ? storeFactoryClass.getName() : null;
  }
}
