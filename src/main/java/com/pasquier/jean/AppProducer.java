package com.pasquier.jean;

import com.pasquier.jean.measurement.Measurement;
import com.pasquier.jean.measurement.MeasurementSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;


import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class AppProducer {

    private final static Logger logger = LoggerFactory.getLogger(AppProducer.class);
    private final String kafka_servers;
    private final String kafka_topic;
    private final int n_devices;
    private final int n_attributes;

    public AppProducer(String kafka_servers, String topic, int devices, int attrs) {
        this.kafka_servers = kafka_servers;
        this.kafka_topic = topic;
        this.n_devices = devices;
        this.n_attributes = attrs;
    }

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();

        for(String k: env.keySet()) {
            System.out.format("[env] %s = %s\n", k, env.get(k));
        }

        AppProducer app = new AppProducer(
                env.getOrDefault("KAFKA_SERVERS", "localhost:9092"),
                env.getOrDefault("KAFKA_TOPIC", "raw_measurements"),
                Integer.parseInt(env.getOrDefault("N_DEVICES", "6")),
                Integer.parseInt(env.getOrDefault("N_ATTRIBUTES", "4"))
        );
        app.start();
    }

    private void start() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafka_servers);
        props.put("acks", "1");
        props.put("retries", 2);
        props.put("linger.ms", 500);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", MeasurementSerializer.class);

        KafkaProducer<String, Measurement> producer = new KafkaProducer<>(props);

        ArrayList<Thread> threads = new ArrayList<>();

        for (int i = 0; i < n_devices; i++) {
            for (int j = 0; j < n_attributes; j++) {
                Measurement m = new Measurement(String.format("device-%04d", i + 1), String.format("attr-%04d", j + 1));
                Thread t = new Thread(new ProducerThread(kafka_topic, producer, m, (int) Math.round(900 + 1000 * Math.random())));
                t.start();
                threads.add(t);
            }
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.info(e.toString());
            }
        }

        producer.close();
    }

}
