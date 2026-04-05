package ru.fisher.tarantool;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.crud.TarantoolCrudClient;
import io.tarantool.client.factory.TarantoolCrudClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class TarantoolServer {
    private static final Logger logger = LoggerFactory.getLogger(TarantoolServer.class);

    private final Server server;
    private final TarantoolCrudClient tarantoolClient;

    public TarantoolServer(int grpcPort, String tarantoolHost, int tarantoolPort,
                           String username) throws Exception {

        logger.info("Connected to Tarantool at {}:{}", tarantoolHost, tarantoolPort);

        // Создание Tarantool CRUD клиента
        this.tarantoolClient = new TarantoolCrudClientBuilder()
                .withHost(tarantoolHost)
                .withPort(tarantoolPort)
                .withUser(username)
                .withPassword(null)
                .withConnectTimeout(5000)
                .withReconnectAfter(1000)
                .build();

        // Проверяем подключение через ping
        try {
            boolean connected = tarantoolClient.ping().get(5, TimeUnit.SECONDS);
            if (connected) {
                logger.info("Successfully connected to Tarantool and pinged");
            } else {
                logger.warn("Connected but ping failed");
            }
        } catch (Exception e) {
            logger.warn("Failed to connect to Tarantool: {}", e.getMessage());
            throw new RuntimeException("Cannot connect to Tarantool", e);
        }

        // Создание gRPC сервера
        KvStoreImpl kvStore = new KvStoreImpl(tarantoolClient);
        this.server = ServerBuilder.forPort(grpcPort)
                .addService(kvStore)
                .build();
    }

    public void start() throws Exception {
        server.start();
        logger.info("gRPC server started on port {}", server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                TarantoolServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Shutdown interrupted: {}", e.getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void stop() throws Exception {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            logger.info("gRPC server stopped");
        }
        if (tarantoolClient != null) {
            try {
                tarantoolClient.close();
                logger.info("Tarantool client closed");
            } catch (Exception e) {
                logger.warn("Error closing Tarantool client: {}", e.getMessage());
            }
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        // Настройки
        int grpcPort = 8080;
        String tarantoolHost = System.getProperty("tarantool.host", "localhost");
        int tarantoolPort = Integer.getInteger("tarantool.port", 3301);
        String username = System.getProperty("tarantool.user", "guest");

        TarantoolServer server = new TarantoolServer(
                grpcPort, tarantoolHost, tarantoolPort, username
        );

        server.start();
        server.blockUntilShutdown();
    }
}
