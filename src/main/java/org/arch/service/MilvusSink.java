package org.arch.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.arch.model.VectorRecord;

import java.util.*;

public class MilvusSink extends RichSinkFunction<VectorRecord> {

    private transient MilvusClientV2 client;

    @Override
    public void open(Configuration parameters) {

        LoadBalancerRegistry.getDefaultRegistry()
                .register(new PickFirstLoadBalancerProvider());

        NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(MilvusClientV2.class.getClassLoader());

            ConnectConfig config = ConnectConfig.builder()
                    .uri("tcp://milvus-standalone:19530")
                    .build();
            client = new MilvusClientV2(config);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void invoke(VectorRecord record, Context context) {
        Gson gson = new Gson();

        List<Long> ids = Collections.singletonList(System.currentTimeMillis());

        List<List<Float>> vectors = Collections.singletonList(record.getVector());

        List<String> texts = Collections.singletonList(record.getText());

        boolean exists = client.hasCollection(
                HasCollectionReq.builder()
                        .collectionName("documents").build()
        );

        System.out.println("Collection exists? " + exists);

        if (!exists) {
            int dimension = record.getVector().size();
            CreateCollectionReq collectionParam = CreateCollectionReq.builder()
                    .collectionName("documents")
                    .dimension(dimension)
                    .build();
            client.createCollection(collectionParam);
        }

        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", ids.get(i)); // Matches default PK

            // Use "vector", NOT "embedding" (unless you define a custom schema)
            row.add("vector", gson.toJsonTree(vectors.get(i)));

            row.addProperty("text", texts.get(i)); // Dynamic field
            data.add(row);
        }


        InsertReq insertReq = InsertReq.builder()
                .collectionName("documents")
                .data(data)
                .build();

        client.insert(insertReq);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}