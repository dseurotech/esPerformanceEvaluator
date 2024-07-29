package com.eurotech.cloud.elastic.rest;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class LowLevelEsExecutor implements EsExecutor {

    private RestClient client;

    public LowLevelEsExecutor(int numberOfInternalThreads) {
        client = RestClient.builder(
                        new HttpHost("localhost", 9200, "http"))

                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        HttpAsyncClientBuilder builder = httpClientBuilder
                                .setDefaultIOReactorConfig(IOReactorConfig.custom()
                                        .setSoKeepAlive(true)
                                        .setIoThreadCount(numberOfInternalThreads) // default Runtime.getRuntime().availableProcessors()
                                        .build());
                        return builder;
                    }
                })
                //                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                //
                //                    @Override
                //                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                //                        return requestConfigBuilder
                //                                .setConnectionRequestTimeout(0)
                //                                .setConnectTimeout(5000)
                //                                .setSocketTimeout(30000); // default 30000
                //                    }
                //                })
                .build();
    }

    @Override
    public String fetchInfo() {
        Request req = new Request("GET", "/");
        req.addParameter("pretty", "true");
        Response getResponse = null;
        try {
            getResponse = client.performRequest(req);
            return EntityUtils.toString(getResponse.getEntity());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String countDocuments() {
        String searchQueryString = "{\n" + "    \"query\" : {\n" + "        \"term\" : { \"user\" : \"kimchy\" }\n"
                + "    }\n" + "}";

        HttpEntity query = new NStringEntity(searchQueryString, ContentType.APPLICATION_JSON);
        Request req;
        req = new Request("POST", "/myIndex/_count");
        req.addParameter("pretty", "true");
        req.setEntity(query);
        Response searchResponse = null;
        try {
            searchResponse = client.performRequest(req);
            return EntityUtils.toString(searchResponse.getEntity());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void performIndexing() {
        try {
            client.performRequest(createPostRequest());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Request createPostRequest() {
        char[] chars = new char[16];
        Arrays.fill(chars, 'a');
        String str = new String(chars);

        chars = new char[10 * 1024];
        Arrays.fill(chars, 'a');
        String body = new String(chars);

        final HttpEntity kimkyUser = new NStringEntity(
                "{\n"
                        + "    \"user\" : \"kimchy\",\n"
                        + "    \"post_date\" : \"2009-11-15T14:12:12\",\n"
                        + "    \"message\" : \"trying out Elasticsearch\",\n"
                        + "    \"str1\" : \"" + str + "\",\n"
                        + "    \"str2\" : \"" + str + "\",\n"
                        + "    \"str3\" : \"" + str + "\",\n"
                        + "    \"str4\" : \"" + str + "\",\n"
                        + "    \"str5\" : \"" + str + "\",\n"
                        + "    \"str6\" : \"" + str + "\",\n"
                        + "    \"str7\" : \"" + str + "\",\n"
                        + "    \"str8\" : \"" + str + "\",\n"
                        + "    \"str9\" : \"" + str + "\",\n"
                        + "    \"str10\" : \"" + str + "\",\n"
                        + "    \"int1\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int2\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int3\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int4\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int5\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int6\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int7\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int8\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int9\" : " + Integer.toString(12345) + ",\n"
                        + "    \"int10\" : " + Integer.toString(12345) + "\n"
                        // Track large messages. With 10Kb body sometimes instert request fails due to buffer overflow.
                        //					+ "    \"body\" : \"" + body + "\"\n"
                        + "}"
                , ContentType.APPLICATION_JSON);
        //		RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        //		RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(
        //				new HttpAsyncResponseConsumerFactory
        //						.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        //		final RequestOptions requestOptions = builder.build();

        final Request postReq = new Request("POST", "/myIndex/_doc");
        //		postReq.setOptions(requestOptions);
        postReq.addParameter("pretty", "true");
        postReq.setEntity(kimkyUser);
        return postReq;
    }

    @Override
    public void dropAndRecreateIndex() {
        try {
            Response searchResponse = client.performRequest(new Request("DELETE", "/myIndex"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
