package io.debezium.server.test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import java.util.List;

@Named("testconsumer")
@ApplicationScoped
public class TestConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        for (ChangeEvent<Object, Object> r : records) {
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
    }
}
