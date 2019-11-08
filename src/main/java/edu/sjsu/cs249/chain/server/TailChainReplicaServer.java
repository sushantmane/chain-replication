// Author: Sushant Mane
// Course: CS-249 - Distributed Computing
// Project:  Chain Replication - Replicated Incremental HashTable Service
////////////////////////////////////////////////////////////////////////////

package edu.sjsu.cs249.chain.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Replicated Incremental HashTable Service Server
 */
public class TailChainReplicaServer {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainReplicaServer.class);
    private Server server;
    private int port;

    public TailChainReplicaServer(int port) {
        this.port = port;
        // create a gRPC server and register Replicated Incremental HashTable Service
        server = ServerBuilder.forPort(port)
                .addService(new TailChainReplicaService())
                .build();
    }

    public void start() throws IOException, InterruptedException {
        // start gRPC server
        server.start();
        LOG.info("Server started successfully on the port: {}", port);
        // to stop gRPC daemon threads, register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Server shutdown is in progress...");
            TailChainReplicaServer.this.stop();
            LOG.info("Server has been stopped successfully.");
        }));
        // wait for the gRPC server to become terminated
        server.awaitTermination();
    }

    // initiate graceful shutdown of the gRPC server
    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 5144;
        TailChainReplicaServer trServer = new TailChainReplicaServer(port);
        trServer.start();
    }
}
