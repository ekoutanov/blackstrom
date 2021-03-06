package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaLedger implements Ledger {
  private static final int POLL_TIMEOUT_MILLIS = 1_000;
  private static final int PIPELINE_BACKOFF_MILLIS = 1;
  private static final int RETRY_BACKOFF_MILLIS = 100;

  private final Kafka<String, Message> kafka;

  private final String topic;

  private final Zlg zlg;

  private final String codecLocator;

  private final ConsumerPipeConfig consumerPipeConfig;
  
  private final int maxConsumerPipeYields;

  private final Producer<String, Message> producer;

  private final ProducerPipe<String, Message> producerPipe;

  private final List<AsyncReceiver<String, Message>> receivers = new ArrayList<>();

  private final List<ConsumerPipe<String, Message>> consumerPipes = new ArrayList<>();
  
  private final List<ShardedFlow> flows = new ArrayList<>(); 
  
  private final boolean printConfig;
  
  private final int attachRetries;
  
  private final WorkerThread retryThread;
  
  private static class RetryTask {
    final Message message;
    final AppendCallback callback;
    
    RetryTask(Message message, AppendCallback callback) {
      this.message = message;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<RetryTask> retryQueue = new NodeQueue<>();
  private final QueueConsumer<RetryTask> retryQueueConsumer = retryQueue.consumer();

  private static class ConsumerOffsets {
    final Object lock = new Object();
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
  }

  /** Maps handler IDs to consumer offsets. */
  private final Map<Integer, ConsumerOffsets> consumers = new HashMap<>();

  private final AtomicInteger nextHandlerId = new AtomicInteger();

  public KafkaLedger(KafkaLedgerConfig config) {
    kafka = config.getKafka();
    topic = config.getTopic();
    zlg = config.getZlg();
    printConfig = config.isPrintConfig();
    consumerPipeConfig = config.getConsumerPipeConfig();
    maxConsumerPipeYields = config.getMaxConsumerPipeYields();
    attachRetries = config.getAttachRetries();
    codecLocator = CodecRegistry.register(config.getCodec());
    retryThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(KafkaLedger.class, "retry", topic))
        .onCycle(this::onRetry)
        .buildAndStart();

    // may be user-specified in config
    final Properties producerDefaults = new PropsBuilder()
        .withSystemDefault("batch.size", 1 << 18)
        .withSystemDefault("linger.ms", 1)
        .withSystemDefault("compression.type", "lz4")
        .withSystemDefault("request.timeout.ms", 10_000)
        .withSystemDefault("retry.backoff.ms", 100)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties producerOverrides = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", 0)
        .with("max.block.ms", Long.MAX_VALUE)
        .build();
    
    if (printConfig) kafka.describeProducer(zlg::i, producerDefaults, producerOverrides);
    producer = kafka.getProducer(producerDefaults, producerOverrides);
    final String producerPipeThreadName = ProducerPipe.class.getSimpleName() + "-" + topic;
    producerPipe = 
        new ProducerPipe<>(config.getProducerPipeConfig(), producer, producerPipeThreadName, zlg::w);
  }
  
  private void onRetry(WorkerThread t) throws InterruptedException {
    final RetryTask retryTask = retryQueueConsumer.poll();
    if (retryTask != null) {
      append(retryTask.message, retryTask.callback);
    } else {
      Thread.sleep(RETRY_BACKOFF_MILLIS);
    }
  }

  @Override
  public void attach(MessageHandler handler) {
    final String groupId = handler.getGroupId();
    final String consumerGroupId;
    final String autoOffsetReset;
    if (groupId != null) {
      consumerGroupId = groupId;
      autoOffsetReset = OffsetResetStrategy.EARLIEST.name().toLowerCase();
    } else {
      consumerGroupId = null;
      autoOffsetReset = OffsetResetStrategy.LATEST.name().toLowerCase();
    }
    
    // may be user-specified in config
    final Properties consumerDefaults = new PropsBuilder()
        .withSystemDefault("session.timeout.ms", 6_000)
        .withSystemDefault("heartbeat.interval.ms", 2_000)
        .withSystemDefault("max.poll.records", 10_000)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties consumerOverrides = new PropsBuilder()
        .with("group.id", consumerGroupId)
        .with("auto.offset.reset", autoOffsetReset)
        .with("enable.auto.commit", false)
        .with("auto.commit.interval.ms", 0)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaMessageDeserializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .build();
    
    if (printConfig) kafka.describeConsumer(zlg::i, consumerDefaults, consumerOverrides);
    final Consumer<String, Message> consumer = kafka.getConsumer(consumerDefaults, consumerOverrides);
    new Retry()
    .withAttempts(attachRetries)
    .withFaultHandler(zlg::w)
    .withErrorHandler(zlg::e)
    .run(() -> {
      if (groupId != null) {
        consumer.subscribe(Collections.singletonList(topic));
        zlg.d("subscribed to topic %s", z -> z.arg(topic));
      } else {
        final List<PartitionInfo> infos = consumer.partitionsFor(topic);
        final List<TopicPartition> partitions = infos.stream()
            .map(i -> new TopicPartition(i.topic(), i.partition()))
            .collect(Collectors.toList());
        zlg.d("infos=%s, partitions=%s", z -> z.arg(infos).arg(partitions));
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        consumer.assign(partitions);
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
          consumer.seek(entry.getKey(), entry.getValue());
        }
      }
    });

    final Integer handlerId;
    final ConsumerOffsets consumerOffsets;
    final Retention retention;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      consumerOffsets = new ConsumerOffsets();
      consumers.put(handlerId, consumerOffsets);
      final ShardedFlow flow = new ShardedFlow();
      retention = flow;
      flows.add(flow);
    } else {
      handlerId = null;
      consumerOffsets = null;
      retention = NopRetention.getInstance();
    }

    final MessageContext context = new DefaultMessageContext(this, handlerId, retention);
    final String consumerPipeThreadName = ConsumerPipe.class.getSimpleName() + "-" + groupId;
    final RecordHandler<String, Message> pipelinedRecordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        final DefaultMessageId messageId = new DefaultMessageId(record.partition(), record.offset());
        final Message message = record.value();
        message.setMessageId(messageId);
        message.setShardKey(record.key());
        message.setShard(record.partition());
        handler.onMessage(context, message);
      }
    };
    final ConsumerPipe<String, Message> consumerPipe = 
        new ConsumerPipe<>(consumerPipeConfig, pipelinedRecordHandler, consumerPipeThreadName);
    consumerPipes.add(consumerPipe);
    final RecordHandler<String, Message> recordHandler = records -> {
      for (int yields = 0;;) {
        final boolean enqueued = consumerPipe.receive(records);

        if (consumerOffsets != null) {
          final Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot;
          synchronized (consumerOffsets.lock) {
            if (! consumerOffsets.offsets.isEmpty()) {
              offsetsSnapshot = consumerOffsets.offsets;
              consumerOffsets.offsets = new HashMap<>(offsetsSnapshot.size());
            } else {
              offsetsSnapshot = null;
            }
          }

          if (offsetsSnapshot != null) {
            zlg.t("Committing offsets %s", z -> z.arg(offsetsSnapshot));
            consumer.commitAsync(offsetsSnapshot, 
                                 (offsets, exception) -> logException(exception, "Error committing offsets %s", offsets));
          }
        }

        if (enqueued) {
          break;
        } else if (yields < maxConsumerPipeYields) {
          yields++;
          Thread.yield();
        } else {
          Thread.sleep(PIPELINE_BACKOFF_MILLIS);
        }
      }
    };

    final String threadName = KafkaLedger.class.getSimpleName() + "-receiver-" + groupId;
    final AsyncReceiver<String, Message> receiver = new AsyncReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, zlg::w);
    receivers.add(receiver);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    final ProducerRecord<String, Message> record = 
        new ProducerRecord<>(topic, message.getShardIfAssigned(), message.getShardKey(), message);
    final Callback sendCallback = (metadata, exception) -> {
      if (exception == null) {
        callback.onAppend(new DefaultMessageId(metadata.partition(), metadata.offset()), null);
      } else if (exception instanceof RetriableException) { 
        logException(exception, "Retriable error publishing %s (queuing in background)", record);
        retryQueue.add(new RetryTask(message, callback));
      } else {
        callback.onAppend(null, exception);
        logException(exception, "Error publishing %s", record);
      }
    };

    producerPipe.send(record, sendCallback);
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    final ConsumerOffsets consumer = consumers.get(handlerId);
    final DefaultMessageId defaultMessageId = (DefaultMessageId) messageId;
    final TopicPartition tp = new TopicPartition(topic, defaultMessageId.getShard());
    final OffsetAndMetadata om = new OffsetAndMetadata(defaultMessageId.getOffset());
    synchronized (consumer.lock) {
      consumer.offsets.put(tp, om);
    }
  }

  private void logException(Exception cause, String messageFormat, Object... messageArgs) {
    if (cause != null) {
      zlg.w(String.format(messageFormat, messageArgs), cause);
    }
  }

  @Override
  public void dispose() {
    Terminator.blank()
    .add(retryThread)
    .add(receivers)
    .add(consumerPipes)
    .add(producerPipe)
    .add(flows)
    .terminate()
    .joinSilently();
    CodecRegistry.deregister(codecLocator);
  }
}
