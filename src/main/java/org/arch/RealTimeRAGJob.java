package org.arch;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.arch.model.VectorRecord;
import org.arch.service.AsyncEmbeddingFunction;
import org.arch.service.MilvusSink;

public class RealTimeRAGJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers("redpanda:9092")
            .setTopics("raw-documents")
            .setGroupId("lumenstream-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> rawStream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    DataStream<VectorRecord> embeddedStream =
        AsyncDataStream.unorderedWait(
            rawStream,
            new AsyncEmbeddingFunction(),
            30,
            TimeUnit.SECONDS,
            50 // Capacity (max concurrent requests)
            );

    embeddedStream.addSink(new MilvusSink()).name("milvus-sink");
    env.execute("Real-time RAG Embedding Pipeline");
  }
}
