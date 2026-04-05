package ru.fisher.tarantool;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fisher.grpc.*;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;


public class TestClient {
    private static final Logger logger = LoggerFactory.getLogger(TestClient.class);
    private final KvServiceGrpc.KvServiceBlockingStub stub;
    private final ManagedChannel channel;

    public TestClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = KvServiceGrpc.newBlockingStub(channel);
        logger.info("Client connected to {}:{}", host, port);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        logger.info("Client shutdown");
    }

    public void testPut() {
        logger.info("\n=== Testing PUT ===");

        // Вставляем со значением
        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("user:1")
                .setValue(ByteString.copyFrom("Ivan Fisher", StandardCharsets.UTF_8))
                .build();
        PutResponse putResponse = stub.put(putRequest);
        logger.info("PUT user:1 -> success: {}", putResponse.getSuccess());

        // Вставляем null
        PutRequest nullRequest = PutRequest.newBuilder()
                .setKey("user:null")
                .setValue(ByteString.EMPTY)
                .build();
        PutResponse nullResponse = stub.put(nullRequest);
        logger.info("PUT user:null (null value) -> success: {}", nullResponse.getSuccess());

        // Меняем ключ
        PutRequest putRequest2 = PutRequest.newBuilder()
                .setKey("user:2")
                .setValue(ByteString.copyFrom("Sergey Brin", StandardCharsets.UTF_8))
                .build();
        stub.put(putRequest2);
        logger.info("PUT user:2 -> success: true");
    }

    public void testGet() {
        logger.info("\n=== Testing GET ===");

        // Получаем существующий ключ
        GetRequest getRequest = GetRequest.newBuilder()
                .setKey("user:1")
                .build();
        GetResponse getResponse = stub.get(getRequest);
        if (getResponse.getFound()) {
            String value = getResponse.getValue().toStringUtf8();
            logger.info("GET user:1 -> found: true, value: {}", value);
        } else {
            logger.info("GET user:1 -> not found");
        }

        // Получаем null значение
        GetRequest nullGetRequest = GetRequest.newBuilder()
                .setKey("user:null")
                .build();
        GetResponse nullGetResponse = stub.get(nullGetRequest);
        logger.info("GET user:null -> found: {}, value present: {}",
                nullGetResponse.getFound(), !nullGetResponse.getValue().isEmpty());

        // Получить не существующий ключ
        GetRequest nonExistent = GetRequest.newBuilder()
                .setKey("non:existent")
                .build();
        GetResponse nonExistentResponse = stub.get(nonExistent);
        logger.info("GET non:existent -> found: {}", nonExistentResponse.getFound());
    }

    public void testRange() {
        logger.info("\n=== Testing RANGE ===");

        // Вставим несколько ключей для промежутка
        for (int i = 1; i <= 10; i++) {
            PutRequest request = PutRequest.newBuilder()
                    .setKey("range:" + i)
                    .setValue(ByteString.copyFrom(("Value " + i).getBytes(StandardCharsets.UTF_8)))
                    .build();
            stub.put(request);
        }
        logger.info("Inserted 10 test keys (range:1 to range:10)");

        // Запрос промежутка от 3 до 7
        RangeRequest rangeRequest = RangeRequest.newBuilder()
                .setFrom("range:3")
                .setTo("range:7")
                .build();

        Iterator<RangeResponse> responses = stub.range(rangeRequest);
        logger.info("Range query results (from 'range:3' to 'range:7'):");
        int count = 0;
        while (responses.hasNext()) {
            RangeResponse response = responses.next();
            String value = response.getValue().toStringUtf8();  // Преобразуем в строку
            logger.info("  {} = {}", response.getKey(), value);
            count++;
        }
        logger.info("Total records in range: {}", count);
    }

    public void testCount() {
        logger.info("\n=== Testing COUNT ===");
        CountResponse response = stub.count(Empty.newBuilder().build());
        logger.info("Total records in database: {}", response.getCount());
    }

    public void testUpdate() {
        logger.info("\n=== Testing UPDATE (overwrite) ===");

        // Получаем значение
        GetRequest getRequest = GetRequest.newBuilder()
                .setKey("user:1")
                .build();
        GetResponse beforeUpdate = stub.get(getRequest);
        logger.info("Before update: {}", beforeUpdate.getValue().toStringUtf8());

        // Обновляем с новым значением
        PutRequest updateRequest = PutRequest.newBuilder()
                .setKey("user:1")
                .setValue(ByteString.copyFrom("Ivan Updated", StandardCharsets.UTF_8))
                .build();
        stub.put(updateRequest);

        // Получаем обновленное значение
        GetResponse afterUpdate = stub.get(getRequest);
        logger.info("After update: {}", afterUpdate.getValue().toStringUtf8());
    }

    public void testDelete() {
        logger.info("\n=== Testing DELETE ===");

        // Удаляем существующий ключ
        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setKey("user:1")
                .build();
        DeleteResponse deleteResponse = stub.delete(deleteRequest);
        logger.info("DELETE user:1 -> success: {}", deleteResponse.getSuccess());

        // Проверяем удаление
        GetRequest getRequest = GetRequest.newBuilder()
                .setKey("user:1")
                .build();
        GetResponse getResponse = stub.get(getRequest);
        logger.info("Verify deletion: found = {}", getResponse.getFound());

        // Попытаемся удалить не существующий ключ
        DeleteRequest deleteNonExistent = DeleteRequest.newBuilder()
                .setKey("non:existent")
                .build();
        DeleteResponse deleteResponse2 = stub.delete(deleteNonExistent);
        logger.info("DELETE non:existent -> success: {}", deleteResponse2.getSuccess());
    }

    public static void main(String[] args) {
        TestClient client = new TestClient("localhost", 8080);

        try {
            logger.info("Starting gRPC client tests...");
            logger.info("====================================");

            client.testPut();
            client.testGet();
            client.testUpdate();
            client.testRange();
            client.testCount();
            client.testDelete();

            // Финальный подсчет после удаления
            logger.info("\n=== Final count ===");
            client.testCount();

            logger.info("\n====================================");
            logger.info("All tests completed successfully!");

        } catch (Exception e) {
            logger.warn("Error during tests: {}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
