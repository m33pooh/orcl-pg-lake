package io.debezium.server.grpc.proto;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.ClientCalls;

public final class IngestionServiceGrpc {
    public static final String SERVICE_NAME = "datalake.cdc.IngestionService";

    private static volatile MethodDescriptor<BatchPayload, BatchResponse> getSendBatchMethod;

    public static MethodDescriptor<BatchPayload, BatchResponse> getSendBatchMethod() {
        MethodDescriptor<BatchPayload, BatchResponse> method;
        if ((method = IngestionServiceGrpc.getSendBatchMethod) == null) {
            synchronized (IngestionServiceGrpc.class) {
                if ((method = IngestionServiceGrpc.getSendBatchMethod) == null) {
                    IngestionServiceGrpc.getSendBatchMethod = method =
                        MethodDescriptor.<BatchPayload, BatchResponse>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "SendBatch"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(BatchPayload.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(BatchResponse.getDefaultInstance()))
                            .build();
                }
            }
        }
        return method;
    }

    public static IngestionServiceBlockingStub newBlockingStub(Channel channel) {
        return IngestionServiceBlockingStub.newStub(FACTORY, channel);
    }

    private static final io.grpc.stub.AbstractStub.StubFactory<IngestionServiceBlockingStub> FACTORY =
        new io.grpc.stub.AbstractStub.StubFactory<IngestionServiceBlockingStub>() {
            @Override
            public IngestionServiceBlockingStub newStub(Channel channel, CallOptions callOptions) {
                return new IngestionServiceBlockingStub(channel, callOptions);
            }
        };

    public static final class IngestionServiceBlockingStub extends AbstractBlockingStub<IngestionServiceBlockingStub> {
        private IngestionServiceBlockingStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected IngestionServiceBlockingStub build(Channel channel, CallOptions callOptions) {
            return new IngestionServiceBlockingStub(channel, callOptions);
        }

        public BatchResponse sendBatch(BatchPayload request) {
            return ClientCalls.blockingUnaryCall(
                getChannel(), getSendBatchMethod(), getCallOptions(), request);
        }
    }
}
