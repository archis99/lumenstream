package org.arch.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.arch.model.VectorRecord;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;

public class AsyncEmbeddingFunction extends RichAsyncFunction<String, VectorRecord> {
    private transient HttpClient httpClient;
    private transient ObjectMapper mapper;
    private String apiKey;

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newHttpClient();
        mapper = new ObjectMapper();
        apiKey = "";
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<VectorRecord> resultFuture) {
        try {
            String body = """
                {
                  "model": "text-embedding-3-small",
                  "input": %s
                }
                """.formatted(mapper.writeValueAsString(input));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.openai.com/v1/embeddings"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::body)
                    .thenAccept(response -> {
                        try {
                            JsonNode root = mapper.readTree(response);
                            JsonNode embeddingNode = root.get("data").get(0).get("embedding");

                            List<Float> vector = mapper.convertValue(
                                    embeddingNode,
                                    mapper.getTypeFactory().constructCollectionType(List.class, Float.class)
                            );

                            resultFuture.complete(
                                    Collections.singleton(new VectorRecord(input, vector))
                            );

                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                        }
                    });

        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }
}
