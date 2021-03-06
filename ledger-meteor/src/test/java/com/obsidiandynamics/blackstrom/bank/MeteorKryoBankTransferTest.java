package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.meteor.*;

@RunWith(Parameterized.class)
public final class MeteorKryoBankTransferTest extends AbstractBankTransferTest {  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  private HazelcastInstance instance;
  
  @Before
  public void before() {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    instance = new TestProvider().createInstance(config);
  }
  
  @After
  public void after() {
    afterBase();
    if (instance != null) {
      instance.getLifecycleService().terminate();
    }
  }
  
  @Override
  protected Ledger createLedger(Guidance guidance) {
    final MeteorLedgerConfig config = new MeteorLedgerConfig()
        .withCodec(new KryoMessageCodec(true, new KryoBankExpansion()))
        .withStreamConfig(new StreamConfig()
                          .withName("stream")
                          .withHeapCapacity(100_000))
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1));
    return new MeteorLedger(instance, config);
  }
}
