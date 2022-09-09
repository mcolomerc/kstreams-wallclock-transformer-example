package org.github.ogomezso.wallclocktransformer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AppTest {

    static Logger logger = LoggerFactory.getLogger(AppTest.class.getName());

    static final String InputTopic1 = "in1";
    static final String OutputTopic = "out";

    final List<TestRecord<String, String>> inputRecords = List.of(
            new TestRecord("1", "h", null, 1000L),
            new TestRecord("1", "e", null, 2000L),
            new TestRecord("2", "w", null, 3000L),
            new TestRecord("2", "o", null, 4000L),
            new TestRecord("1", "l", null, 5000L),
            new TestRecord("1", "l", null, 6000L),
            new TestRecord("1", "o", null, 7000L),
            new TestRecord("1", "H", null, 15000L)
    );

    final List<TestRecord<String, String>> expectedRecords = List.of(
            new TestRecord("1-Window{startMs=1000, endMs=1000}", "h", null, Instant.ofEpochMilli(1L).plusMillis(1000L)),
            new TestRecord("1-Window{startMs=1000, endMs=2000}", "he", null, Instant.ofEpochMilli(1L).plusMillis(2000L)),
            new TestRecord("2-Window{startMs=3000, endMs=3000}", "w", null, Instant.ofEpochMilli(1L).plusMillis(3000L)),
            new TestRecord("2-Window{startMs=3000, endMs=4000}", "wo", null, Instant.ofEpochMilli(1L).plusMillis(4000L)),
            new TestRecord("1-Window{startMs=1000, endMs=5000}", "hel", null, Instant.ofEpochMilli(1L).plusMillis(5000L)),
            new TestRecord("1-Window{startMs=1000, endMs=6000}", "hell", null, Instant.ofEpochMilli(1L).plusMillis(6000L)),
            new TestRecord("1-Window{startMs=1000, endMs=7000}", "hello", null, Instant.ofEpochMilli(1L).plusMillis(7000L))
    );

    @Test
    public void shouldConcatenateWithReduce() {
        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(createTopology(), getConfig(), Instant.ofEpochMilli(1L))) {
            final TestInputTopic<String, String> input = topologyTestDriver
                    .createInputTopic(InputTopic1, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> output = topologyTestDriver
                    .createOutputTopic(OutputTopic, new StringDeserializer(), new StringDeserializer());

            inputRecords.forEach((record -> {
                input.pipeInput(record.key(), record.value(), record.timestamp());
                topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));
            }));
            var actualRecords = output.readRecordsToList();
            actualRecords.forEach(System.out::println);
            assertThat(actualRecords).hasSameElementsAs(expectedRecords);

        }
    }

    private Topology createTopology(){
        final var builder = new StreamsBuilder();
        builder.addStateStore(Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("store"),
                Serdes.String(),
                Serdes.String()));
        builder.stream(InputTopic1, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> logger.info("input values for key [" + k + "] => [" + v + "]"))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5)))
                .reduce((v1, v2) -> v1 + v2)
                .toStream()
                .map((Windowed<String> key, String value) -> new KeyValue<>(key.key() + "-" + key.window(), value))
                .transform(AggregateTransformer::new, Named.as("aggregate-transformer"), "store")
                .peek((k, v) -> logger.info("Produced values for key [" + k + "] => [" + v + "]"))
                .to(OutputTopic,
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        return builder.build();

    }
    private Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                CreatedAtTimestampExtractor.class.getName());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/aggregated-values");
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-wallclock-app");
        return config;
    }


}
