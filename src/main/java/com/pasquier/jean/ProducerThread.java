package com.pasquier.jean;

import com.pasquier.jean.measurement.Measurement;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ProducerThread.class);
    private KafkaProducer<String, Measurement> producer;
    private Measurement m;
    private String kafka_topic;
    private int freq_ms;

    public ProducerThread(String kafka_topic, KafkaProducer<String, Measurement> producer, Measurement m, int freq_ms) {
        this.kafka_topic = kafka_topic;
        this.producer = producer;
        this.m = m;
        this.freq_ms = freq_ms;
    }

    @Override
    public void run() {

        boolean ended = false;

        while (!ended) {

            m.setTs(System.currentTimeMillis());
            m.setValue(100 * Math.random());

            send_one(producer, m.retrieveKey(), m);

            try {
                Thread.sleep(freq_ms);
            } catch (InterruptedException e) {
                logger.error(e.toString());
                ended = true;
            }
        }

    }


    private void send_one(KafkaProducer<String, Measurement> producer, String key, Measurement value) {
        logger.info(String.format("Sending Record to %s: '%s': '%s'", kafka_topic, key, value));
        producer.send(new ProducerRecord<>(kafka_topic, key, value));
    }
}
