package ru.fisher.tarantool;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fisher.grpc.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentLoadTest.class);

    public static class Worker implements Runnable {
        private final KvServiceGrpc.KvServiceBlockingStub stub;
        private final int startId;
        private final int count;
        private final AtomicLong successCount;

        public Worker(String host, int port, int startId, int count, AtomicLong successCount) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            this.stub = KvServiceGrpc.newBlockingStub(channel);
            this.startId = startId;
            this.count = count;
            this.successCount = successCount;
        }

        @Override
        public void run() {
            for (int i = 0; i < count; i++) {
                try {
                    int id = startId + i;
                    PutRequest request = PutRequest.newBuilder()
                            .setKey("thread_key_" + id)
                            .setValue(ByteString.copyFrom(("Value_" + id).getBytes(StandardCharsets.UTF_8)))
                            .build();
                    stub.put(request);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    logger.warn("Error in worker: {}", e.getMessage());
                }
            }
        }
    }

    public void runConcurrentTest(int totalRecords, int threadCount) throws InterruptedException {
        logger.info("\n=== Concurrent PUT Test ===");
        logger.info("  Total records: {}", totalRecords);
        logger.info("  Threads: {}", threadCount);

        int recordsPerThread = totalRecords / threadCount;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            AtomicLong successCount = new AtomicLong(0);

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < threadCount; i++) {
                int startId = i * recordsPerThread;
                executor.submit(new Worker("localhost", 8080, startId, recordsPerThread, successCount));
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            logger.info("\n=== Results ===");
            logger.info("  Successful writes: {}", successCount.get());
            logger.info("  Time: {} ms", duration);
            logger.info("  Average rate: {} ops/sec", String.format("%.0f", successCount.get() * 1000.0 / duration));
            logger.info("  Per thread: {} records/thread", successCount.get() / threadCount);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConcurrentLoadTest test = new ConcurrentLoadTest();

        // Тест с 100,000 записей, 10 потоков
        test.runConcurrentTest(100_000, 10);
    }
}
