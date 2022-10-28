package com.pasquier.jean;

import com.pasquier.jean.measurement.Measurement;
import com.pasquier.jean.measurement.MeasurementDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AppConsumer {

    public void start() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", MeasurementDeserializer.class);

        KafkaConsumer<String, Measurement> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("measurements-101"));

        while (true) {
            ConsumerRecords<String, Measurement> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Measurement> record : records)
                System.out.printf("[%d] <%s> %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void main(String[] args) {

        AppConsumer app = new AppConsumer();
        app.start();

    }

}
