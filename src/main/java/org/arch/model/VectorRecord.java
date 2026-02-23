package org.arch.model;

import java.util.List;

public class VectorRecord {
    public String text;
    public List<Float> vector;

    public VectorRecord() {} // Flink needs default constructor
    public VectorRecord(String text, List<Float> vector) {
        this.text = text;
        this.vector = vector;
    }

    public String getText() {
        return text;
    }

    public List<Float> getVector() {
        return vector;
    }
}
