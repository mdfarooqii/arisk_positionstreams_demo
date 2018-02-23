package com.ubs.risk.arisk.stream.position;

import com.ubs.risk.arisk.stream.position.model.PositionKey;
import com.ubs.risk.arisk.stream.position.model.PositionValue;
import com.ubs.risk.arisk.stream.position.serializer.JsonDeserializer;
import com.ubs.risk.arisk.stream.position.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;

import java.util.Arrays;
import java.util.Properties;

public class PositionStream {

    public static void main(String[] args) {
        System.out.println("Hello world");

        String inputTopicName = args [0];
        String outputTopicName = args [1];

        JsonSerializer<PositionKey> positionKeyJsonSerializer = new JsonSerializer<PositionKey>(PositionKey.class);
        JsonDeserializer<PositionKey> positionKeyJsonDeserializer = new JsonDeserializer<PositionKey>(PositionKey.class);
        Serde<PositionKey> positionKeySerde = Serdes.serdeFrom(positionKeyJsonSerializer,positionKeyJsonDeserializer);

        JsonSerializer<PositionValue> positionValueJsonSerializer = new JsonSerializer<PositionValue>(PositionValue.class);
        JsonDeserializer<PositionValue> positionValueJsonDeserializer = new JsonDeserializer<PositionValue>(PositionValue.class);
        Serde<PositionValue> positionValueSerde = Serdes.serdeFrom(positionKeyJsonSerializer,positionKeyJsonDeserializer);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "position-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, positionKeySerde.getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, positionValueSerde.getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<PositionKey, PositionValue> positions = builder.stream(inputTopicName);

        KTable<PositionKey, PositionValue> positionKVTable = positions
                // 5 - group by key before aggregation
                .groupByKey().reduce(new Reducer<PositionValue>() {
                    @Override
                    public PositionValue apply(PositionValue positionValue, PositionValue v1) {
                        return positionValue;
                    }
                });


        // in order to write the results back to kafka
        positionKVTable.to( positionKeySerde,positionValueSerde,outputTopicName);


        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


        // print the topology every 20 seconds for learning purposes
        while(true){
            System.out.println(streams.toString());
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
