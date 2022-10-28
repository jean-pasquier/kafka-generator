package com.pasquier.jean.measurement;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class MeasurementDeserializer implements Deserializer<Measurement> {
    private Logger logger = LoggerFactory.getLogger(MeasurementDeserializer.class);

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Measurement deserialize(String s, byte[] bytes) {
        logger.trace(String.format("%s, %s", s, bytes.toString()));

        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        try {
            return jsonMapper.readValue(bytes, Measurement.class);
        } catch (IOException e) {
            logger.error(e.toString());
            return null;
        }
    }

    @Override
    public void close() {

    }


    public static void main(String[] args) {

        MeasurementDeserializer d = new MeasurementDeserializer();

        String json = "{ \"attribute\" : \"attr\", \"device\" : \"device\", \"value\" : 12.93, \"ts\" : 1620856320}";

        Measurement m = d.deserialize(json, null, json.getBytes());

    }

}
