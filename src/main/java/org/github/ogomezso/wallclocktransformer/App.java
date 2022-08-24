package org.github.ogomezso.wallclocktransformer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App extends StreamApp {
    static Logger logger = LoggerFactory.getLogger(App.class.getName());

    public static void main(String[] args) throws Exception {
        Properties extraProperties = new Properties();
        App streamApp = new App();
                extraProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            CreatedAtTimestampExtractor.class.getName());
        extraProperties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/aggregated-values");
        extraProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "example-wallclock-app");
        streamApp.run(args, extraProperties);

    }

    @Override
    protected void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        builder.addStateStore(Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("store"),
                Serdes.String(),
                Serdes.String()));
        KStream<String, String> stream = builder.stream("test",
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("test-store"));

        stream
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5)))
                .reduce((v1,v2) -> v1+v2)
                .toStream(Named.as("reduced-value"))
                .map((Windowed<String> key, String value) -> new KeyValue<>(key.key(),value))
                .transform(AggregateTransformer::new, Named.as("aggregate-transformer"), "store")
                .peek((k, v) -> logger.info("Sending aggregated value for key  [{}] => [{}]", k, v))
                .to("output-test",
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

    }
}
