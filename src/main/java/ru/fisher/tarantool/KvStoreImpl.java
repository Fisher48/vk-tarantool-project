package ru.fisher.tarantool;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.tarantool.client.crud.TarantoolCrudClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fisher.grpc.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KvStoreImpl extends KvServiceGrpc.KvServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(KvStoreImpl.class.getName());

    private final TarantoolCrudClient client;

    public KvStoreImpl(TarantoolCrudClient client) {
        this.client = client;
        logger.info("KV Store initialized");

        // Проверяем, что пространство существует
        try {
            String checkLua = "return box.space.KV ~= nil";
            Object result = client.eval(checkLua).get();
            logger.info("Space KV exists: {}", result);
        } catch (Exception e) {
            logger.warn("Could not verify space KV: {}", e.getMessage());
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            String key = request.getKey();
            byte[] value = request.getValue().toByteArray();
            Object val = (value.length == 0) ? null : value;

            logger.info("PUT: key='{}', valueIsNull={}", key, val == null);

            String lua = "box.space.KV:replace({...})";
            client.eval(lua, java.util.Arrays.asList(key, val)).get();

            responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.warn("PUT error: {}", e.getMessage());
            e.printStackTrace();
            responseObserver.onError(Status.INTERNAL
                    .withDescription("PUT failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            String key = request.getKey();
            logger.info("GET: key='{}'", key);

            String lua = "return box.space.KV:get({...})";
            List<?> result = client.eval(lua, List.of(key)).get().get();

            logger.info("GET raw result: {}", result);

            GetResponse.Builder builder = GetResponse.newBuilder();
            boolean found = false;
            byte[] value = null;

            if (result != null && !result.isEmpty()) {
                Object first = result.getFirst();
                if (first instanceof List<?> tuple && tuple.size() >= 2) {
                    found = true;
                    Object valueObj = tuple.get(1);
                    if (valueObj instanceof byte[]) {
                        value = (byte[]) valueObj;
                        logger.info("GET found byte[] value, length={}", value.length);
                    } else if (valueObj == null) {
                        logger.info("GET found null value");
                    } else {
                        logger.info("GET found value of type: {}", valueObj.getClass());
                    }
                }

            }

            if (found) {
                if (value != null && value.length > 0) {
                    builder.setValue(com.google.protobuf.ByteString.copyFrom(value));
                }
                builder.setFound(true);
                logger.info("GET successful for key='{}'", key);
            } else {
                builder.setFound(false);
                logger.info("GET not found for key='{}'", key);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.warn("GET error: {}", e.getMessage());
            e.printStackTrace();
            responseObserver.onError(Status.INTERNAL
                    .withDescription("GET failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            String key = request.getKey();
            logger.info("DELETE: key='{}'", key);

            String lua = "return box.space.KV:delete({...})";
            List<?> result = client.eval(lua, List.of(key)).get().get();

            boolean success = result != null && !result.isEmpty();
            responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(success).build());
            responseObserver.onCompleted();
            logger.info("DELETE success: {}", success);

        } catch (Exception e) {
            logger.warn("DELETE error: {}", e.getMessage());
            e.printStackTrace();
            responseObserver.onError(Status.INTERNAL
                    .withDescription("DELETE failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        try {
            String from = request.getFrom();
            String to = request.getTo();

            logger.info("RANGE: from='{}', to='{}'", from, to);

            // Получаем все ключи в диапазоне
            for (int i = Integer.parseInt(from.split(":")[1]);
                 i <= Integer.parseInt(to.split(":")[1]); i++) {
                String key = "range:" + i;

                // Используем существующий метод get для получения значения
                String lua = "return box.space.KV:get({...})";
                List<?> result = client.eval(lua, List.of(key)).get().get();

                if (result != null && !result.isEmpty()) {
                    Object first = result.getFirst();
                    if (first instanceof List<?> tuple && tuple.size() >= 2 && tuple.get(1) != null) {
                            byte[] value = convertToBytes(tuple.get(1));
                            RangeResponse.Builder builder = RangeResponse.newBuilder()
                                    .setKey(key)
                                    .setValue(ByteString.copyFrom(value));
                            responseObserver.onNext(builder.build());
                        }

                }
            }

            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.warn("RANGE error: {}", e.getMessage());
            e.printStackTrace();
            responseObserver.onError(Status.INTERNAL
                    .withDescription("RANGE failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private byte[] convertToBytes(Object valueObj) {
        switch (valueObj) {
            case null -> {
                return new byte[0];
            }
            case byte[] bytes1 -> {
                return bytes1;
            }
            case String string -> {
                return string.getBytes(StandardCharsets.UTF_8);
            }
            case ByteBuffer bb -> {
                byte[] bytes = new byte[bb.remaining()];
                bb.get(bytes);
                return bytes;
            }
            default -> {}
        }
        // Для MessagePack и других типов — сериализация в строку
        return valueObj.toString().getBytes(StandardCharsets.UTF_8);
    }


    @Override
    public void count(Empty request, StreamObserver<CountResponse> responseObserver) {
        try {
            logger.info("COUNT");

            String lua = "return box.space.KV:count()";
            List<?> result = client.eval(lua).get().get();

            long count = 0;
            if (result != null && !result.isEmpty()) {
                Object first = result.getFirst();
                if (first instanceof Number) {
                    count = ((Number) first).longValue();
                }
            }

            responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
            logger.info("COUNT = {}", count);

        } catch (Exception e) {
            logger.warn("COUNT error: {}", e.getMessage());
            e.printStackTrace();
            responseObserver.onError(Status.INTERNAL
                    .withDescription("COUNT failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
