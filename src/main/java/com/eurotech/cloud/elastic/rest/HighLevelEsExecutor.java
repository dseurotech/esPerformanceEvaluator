package com.eurotech.cloud.elastic.rest;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class HighLevelEsExecutor implements EsExecutor {

    private final RestHighLevelClient client;

    public HighLevelEsExecutor(int numberOfInternalThreads) {
        client = new RestHighLevelClient(RestClient.builder(
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
        );
    }

    @Override
    public String fetchInfo() {
        try {
            return client.info(RequestOptions.DEFAULT).toString();
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
        try {
            return client.count(new CountRequest("myIndex"), RequestOptions.DEFAULT).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void performIndexing() {
        try {
            client.index(new IndexRequest("myIndex").source(buildPayload()), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static XContentBuilder buildPayload() throws IOException {
        char[] chars = new char[16];
        Arrays.fill(chars, 'a');
        String str = new String(chars);

        chars = new char[10 * 1024];
        Arrays.fill(chars, 'a');
        String body = new String(chars);
        return XContentFactory
                .jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("post_date", "2009-11-15T14:12:12")
                .field("message", "trying out Elasticsearch")
                .field("str1", str)
                .field("str2", str)
                .field("str3", str)
                .field("str4", str)
                .field("str5", str)
                .field("str6", str)
                .field("str7", str)
                .field("str8", str)
                .field("str9", str)
                .field("str10", str)
                .field("int1", Integer.toString(12345))
                .field("int2", Integer.toString(12345))
                .field("int3", Integer.toString(12345))
                .field("int4", Integer.toString(12345))
                .field("int5", Integer.toString(12345))
                .field("int6", Integer.toString(12345))
                .field("int7", Integer.toString(12345))
                .field("int8", Integer.toString(12345))
                .field("int9", Integer.toString(12345))
                .field("int10", Integer.toString(12345))
                .endObject();
    }

    @Override
    public void dropAndRecreateIndex() {
        try {
            if (client.indices().exists(new GetIndexRequest("myIndex"), RequestOptions.DEFAULT)) {
                client.indices().delete(new DeleteIndexRequest("myIndex"), RequestOptions.DEFAULT);
            }
            createIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //        System.out.println(clientPool.get(0).indices().create(new CreateIndexRequest("myIndex"), RequestOptions.DEFAULT));
    }

    void createIndex() throws IOException {
        Request createIdxReq = new Request("PUT", "/myIndex");
        createIdxReq.addParameter("pretty", "true");
        createIdxReq.setEntity(new NStringEntity(
                "{\n"
                        + "    \"settings\" : {\n"
                        + "        \"number_of_shards\" : 3,\n"
                        + "        \"number_of_replicas\" : 0\n"
                        //					+ "        \"refresh_interval\" : \"5s\"\n"
                        + "    }\n"
                        + "}"
                , ContentType.APPLICATION_JSON));
        client.getLowLevelClient().performRequest(createIdxReq);
    }
}
