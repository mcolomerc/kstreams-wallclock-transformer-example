package org.github.ogomezso.wallclocktransformer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamApp {
    private static Logger logger = LoggerFactory.getLogger(StreamApp.class.getName());
    protected Properties properties;
    protected KafkaStreams kafkaStreams;

    protected void run(String[] args) throws Exception {
        run(args, null);
    }

    protected void run(String[] args, Properties extraProperties) throws Exception {
        properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            properties.load(new StreamApp().getClass().getResourceAsStream("/kafka.properties"));
        }

        if (extraProperties != null) {
            extraProperties.forEach((k, v) -> {
                properties.put(k, v);
            });
        }

        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);
        Topology topology = builder.build();
        logger.info(topology.describe().toString());
        
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setUncaughtExceptionHandler((e) -> {
            logger.error(null, e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
       }

    protected void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        throw new UnsupportedOperation("Not implemented");
    }
}
