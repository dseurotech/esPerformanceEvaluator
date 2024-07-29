package com.eurotech.cloud.elastic.rest;

public interface EsExecutor {

    String fetchInfo();

    void close();

    String countDocuments();

    void performIndexing();

    void dropAndRecreateIndex();
}
