package com.pasquier.jean.measurement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class MeasurementSerializer implements Serializer<Measurement> {
    private Logger logger = LoggerFactory.getLogger(MeasurementSerializer.class);


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public byte[] serialize(String s, Measurement measurement) {
        ObjectMapper jsonMapper = new ObjectMapper();
        try {
            return jsonMapper.writeValueAsBytes(measurement);
        } catch (JsonProcessingException e) {
            logger.error(e.toString());
            return null;
        }
    }
}
