package edu.sjsu.cs249.chain.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TailReplicaServer {

    private static final Logger LOG = LoggerFactory.getLogger(TailReplicaServer.class);
    private Server server;

    public TailReplicaServer(int port) {
        server = ServerBuilder.forPort(port)
                .addService(new TailReplicaService())
                .build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 5144;
        TailReplicaServer trServer = new TailReplicaServer(port);
        trServer.start();
    }
}
