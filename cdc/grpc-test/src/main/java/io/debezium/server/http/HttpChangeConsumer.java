package io.debezium.server.http;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.grpc.proto.BatchPayload;
import io.debezium.server.grpc.proto.BatchResponse;
import io.debezium.server.grpc.proto.IngestionServiceGrpc;
import io.debezium.server.grpc.proto.RecordPayload;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("http")
@ApplicationScoped
public class HttpChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpChangeConsumer.class);

    private ManagedChannel channel;
    private IngestionServiceGrpc.IngestionServiceBlockingStub stub;
    private String appName = "default";

    @PostConstruct
    void connect() {
        Config config = ConfigProvider.getConfig();
        String host = config.getOptionalValue("debezium.sink.grpc.host", String.class).orElse("lake-receiver");
        int port = config.getOptionalValue("debezium.sink.grpc.port", Integer.class).orElse(50051);
        this.appName = config.getOptionalValue("debezium.sink.grpc.app.name", String.class).orElse("app");

        LOGGER.info(">>> Initializing Debezium High-Performance gRPC Client pointing to {}:{} for app '{}' <<<", host, port, appName);

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();
        this.stub = IngestionServiceGrpc.newBlockingStub(channel);
        LOGGER.info(">>> Debezium gRPC Stub connected successfully <<<");
    }

    @PreDestroy
    void close() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.warn("Exception closing gRPC channel: {}", e.getMessage());
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        if (records == null || records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        BatchPayload.Builder batchBuilder = BatchPayload.newBuilder().setAppName(this.appName);

        for (ChangeEvent<Object, Object> record : records) {
            String dest = record.destination() != null ? record.destination() : "";
            String keyStr = record.key() != null ? String.valueOf(record.key()) : "";
            String valStr = record.value() != null ? String.valueOf(record.value()) : "";

            batchBuilder.addRecords(RecordPayload.newBuilder()
                    .setDestination(dest)
                    .setKey(keyStr)
                    .setValue(valStr)
                    .build());
        }

        BatchPayload batch = batchBuilder.build();
        try {
            BatchResponse response = stub.withDeadlineAfter(30, TimeUnit.SECONDS).sendBatch(batch);
            if (!response.getSuccess()) {
                LOGGER.error("gRPC Batch Ingestion failed on receiver: {}", response.getError());
                throw new RuntimeException("Receiver rejected batch: " + response.getError());
            }

            for (ChangeEvent<Object, Object> r : records) {
                committer.markProcessed(r);
            }
            committer.markBatchFinished();
        } catch (Exception e) {
            LOGGER.error("Error sending gRPC batch of {} records: {}", records.size(), e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
