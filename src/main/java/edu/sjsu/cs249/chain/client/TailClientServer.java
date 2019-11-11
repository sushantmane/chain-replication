package edu.sjsu.cs249.chain.client;

import com.google.common.eventbus.EventBus;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TailClientServer {

    private static final Logger LOG = LoggerFactory.getLogger(TailClientServer.class);

    private Server server;

    public TailClientServer(EventBus eventBus, int port, ConcurrentMap<Integer, Boolean> map) {
        server = ServerBuilder.forPort(port)
                .addService(new TailClientService(eventBus, map))
                .build();
    }

    private TailClientServer() {}

    public void start() throws IOException {
        server.start();
        LOG.info("TailClientServer started on port: {}", server.getPort());
        System.out.println("TailClientServer is listening on port: " + server.getPort());
        // add shutdown hook to jvm
        Runtime.getRuntime().addShutdownHook(new Thread(TailClientServer.this::stop));
    }

    public void awaitTermination() throws InterruptedException {
        // wait for the gRPC server to become
        server.awaitTermination();
    }

    // initiate graceful shutdown of the gRPC server
    private void stop() {
        LOG.info("TailClientServer shutdown is in progress...");
        server.shutdown();      // stop grpc server
        try {
            server.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            server.shutdownNow();
        } finally {
            if (server.isTerminated()) {
                LOG.info("TailClientServer has been stopped.");
                System.out.println("TailClientServer has been stopped.");
            }
        }
    }

    public boolean isTerminated() {
        return server.isTerminated();
    }
}