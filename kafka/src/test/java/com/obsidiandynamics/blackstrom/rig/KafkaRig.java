package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.indigo.util.PropertyUtils.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.jgroups.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.props.*;
import com.obsidiandynamics.indigo.util.*;

public final class KafkaRig {
  private static final Properties common = new Properties();
  
  private static final String cluster = getOrSet(common, "rig.cluster", String::valueOf, "rig");
  private static final String bootstrapServers = getOrSet(common, "bootstrap.servers", String::valueOf, "localhost:9092");
  private static final boolean asyncProducer = getOrSet(common, "rig.async.producer", Boolean::valueOf, false);
  private static final boolean asyncConsumer = getOrSet(common, "rig.async.consumer", Boolean::valueOf, true);
  
  private static final Logger log = LoggerFactory.getLogger(KafkaRig.class);
  
  private static final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers(bootstrapServers);

  private static final String topic = 
      TestTopic.of(KafkaRig.class, "kryo", KryoMessageCodec.ENCODING_VERSION, cluster);
  
  private static void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.ensureExists(TestTopic.newOf(topic), KafkaTimeouts.TOPIC_CREATE);
    }
  }
  
  private static void printProps(Properties props) {
    PropertyFormat.printStandard(log::info, "Rig properties", props, "rig.");
  }
  
  private static Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withProducerPipeConfig(new ProducerPipeConfig().withAsync(asyncProducer))
                           .withConsumerPipeConfig(new ConsumerPipeConfig().withAsync(asyncConsumer))
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(topic)
                           .withCodec(new KryoMessageCodec(true, new KryoBankExpansion()))
                           .withPrintConfig(true));
  }
  
  private static JChannel createChannel() throws Exception {
    return Group.newUdpChannel(null);
  }
  
  public static final class Initiator {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(common);
      final long _runs = getOrSet(props, "rig.runs", Long::valueOf, 1_000_000L);
      final int _backlogTarget = getOrSet(props, "rig.backlog", Integer::valueOf, 10_000);
      final int cycles = getOrSet(props, "rig.cycles", Integer::valueOf, 1);
      printProps(props);
      
      before();
      
      for (int cycle = 0; cycle < cycles; cycle++) {
        if (cycles != 1) {
          log.info("——");
          log.info("Cycle #{}/{}", cycle + 1, cycles);
        }
        
        new InitiatorRig.Config() {{
          log = KafkaRig.log;
          ledgerFactory = KafkaRig::createLedger;
          channelFactory = KafkaRig::createChannel;
          clusterName = KafkaRig.cluster;
          runs = _runs;
          backlogTarget = _backlogTarget;
        }}.create().run();
      }
    }
  }
  
  public static final class Cohort {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(common);
      final String _branchId = getOrSet(props, "rig.branch.id", String::valueOf, null);
      assertNotNull("rig.branch.id not set", _branchId);
      printProps(props);
      before();
      
      new CohortRig.Config() {{
        log = KafkaRig.log;
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.cluster;
        branchId = _branchId;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
  
  public static final class Monitor {
    public static void main(String[] args) throws Exception {
      before();
      
      new MonitorRig.Config() {{
        log = KafkaRig.log;
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.cluster;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
}
