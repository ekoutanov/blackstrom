package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;
import com.obsidiandynamics.blackstrom.worker.Terminator;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractPubSubTest {
  /*  
   *  Simulates a slow system by creating auxiliary spinning threads, thereby thrashing the scheduler. Zero means no auxiliary load. 
   */
  private static final int MIN_AUX_LOAD_THREADS = 0;
  private static final int MAX_AUX_LOAD_THREADS = 0;
  private final List<WorkerThread> auxLoadThreads = new ArrayList<>();
  
  protected HazelcastProvider defaultProvider;
  
  protected final Set<HazelcastInstance> instances = new HashSet<>();
  
  protected final Set<Terminable> terminables = new HashSet<>();

  protected final Timesert wait = Wait.SHORT;
  
  @Before
  public final void beforeBase() {
    final int auxThreads = (int) (Math.random() * (MAX_AUX_LOAD_THREADS - MIN_AUX_LOAD_THREADS + 1)) + MIN_AUX_LOAD_THREADS;
    for (int i = 0; i < auxThreads; i++) {
      auxLoadThreads.add(WorkerThread.builder().onCycle(t -> {}).buildAndStart());
    }
    
    defaultProvider = new TestHazelcastProvider();
  }
  
  @After
  public final void afterBase() {
    Terminator.blank()
    .add(terminables)
    .add(auxLoadThreads)
    .terminate()
    .joinSilently();
    terminables.clear();
    auxLoadThreads.clear();
    instances.forEach(h -> h.getLifecycleService().terminate());
    instances.clear();
  }
  
  protected final HazelcastInstance newGridInstance() {
    return newInstance(GridHazelcastProvider.getInstance());
  }
  
  protected final HazelcastInstance newInstance() {
    return newInstance(defaultProvider);
  }
  
  protected final HazelcastInstance newInstance(HazelcastProvider provider) {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    return register(provider.createInstance(config), instances);
  }
  
  protected final DefaultPublisher configurePublisher(PublisherConfig config) {
    return configurePublisher(newInstance(), config);
  }
  
  protected final DefaultPublisher configurePublisher(HazelcastInstance instance, PublisherConfig config) {
    return register(Publisher.createDefault(instance, config), terminables);
  }
  
  protected final DefaultSubscriber configureSubscriber(SubscriberConfig config) {
    return configureSubscriber(newInstance(), config);
  }
  
  protected final DefaultSubscriber configureSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    return register(Subscriber.createDefault(instance, config), terminables);
  }
  
  protected final Receiver createReceiver(Subscriber subscriber, RecordHandler recordHandler, int pollTimeoutMillis) {
    return register(subscriber.createReceiver(recordHandler, pollTimeoutMillis), terminables);
  }
  
  protected static final <T> T register(T item, Collection<? super T> container) {
    container.add(item);
    return item;
  }
  
  protected static final String randomGroup() {
    final UUID random = UUID.randomUUID();
    return "group-" + Long.toHexString(random.getMostSignificantBits() ^ random.getLeastSignificantBits());
  }
  
  protected static final ErrorHandler mockErrorHandler() {
    final ErrorHandler mock = mock(ErrorHandler.class);
    doAnswer(invocation -> {
      final String summary = invocation.getArgument(0);
      final Throwable error = invocation.getArgument(1);
      final Logger log = LoggerFactory.getLogger(AbstractPubSubTest.class);
      log.warn(summary, error);
      return null;
    }).when(mock).onError(any(), any());
    return mock;
  }
  
  protected static final void verifyNoError(ErrorHandler... mockErrorHandlers) {
    Arrays.stream(mockErrorHandlers).forEach(AbstractPubSubTest::verifyNoError);
  }
  
  protected static final void verifyNoError(ErrorHandler mockErrorHandler) {
    verify(mockErrorHandler, never()).onError(any(), any());
  }
}