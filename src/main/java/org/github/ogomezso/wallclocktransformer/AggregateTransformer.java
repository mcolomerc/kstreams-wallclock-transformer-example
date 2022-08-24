package org.github.ogomezso.wallclocktransformer;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateTransformer implements Transformer<String, String, KeyValue<String, String>> {

  static Logger logger = LoggerFactory.getLogger(App.class.getName());

  private TimestampedKeyValueStore<String, String> store;
  private ProcessorContext localContext;
  private Cancellable cancel;

  @Override
  public void init(ProcessorContext context) {
    store = context.getStateStore("store");
    localContext = context;
    cancel = localContext.schedule(Duration.ofSeconds(5),
        PunctuationType.WALL_CLOCK_TIME,
        (timestamp) -> punctuate(timestamp));
  }

  @Override
  public KeyValue<String, String> transform(String key, String value) {

    store.put(key, ValueAndTimestamp.make(value, localContext.timestamp()));

    return null;
  }

  private void punctuate(long timestamp) {
    var recordIterator = store.all();
    try {
      while (recordIterator.hasNext()) {
        var record = recordIterator.next();

        if (timestamp > record.value.timestamp()) {
          localContext.forward(record.key, record.value.value());
          store.delete(record.key);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      store.flush();
      recordIterator.close();
    }
  }

  @Override
  public void close() {

    logger.info("closing");
    cancel.cancel();
  }
}
