package com.pasquier.jean.measurement;

public class Measurement {
    private String device;
    private String attribute;
    private long ts = 0;
    private Double value = null;

    public Measurement() {
        this.device = "UNKNOWN_DEVICE";
        this.attribute = "UNKNOWN_ATTRIBUTE";
    }

    public Measurement(String device, String attribute, long ts, Double value) {
        this.device = device;
        this.attribute = attribute;
        this.ts = ts;
        this.value = value;
    }

    public Measurement(String device, String attribute) {
        this.device = device;
        this.attribute = attribute;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String retrieveKey() {
        return device + "/" + attribute;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public long getTs() {
        return ts;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Measurement(" + device + ", " + attribute + ", " + ts + ", " + value + ")";
    }

    public static void main(String[] args) {
        Measurement m = new Measurement("attr", "device", 1620856320, 12.88837);
    }

}

