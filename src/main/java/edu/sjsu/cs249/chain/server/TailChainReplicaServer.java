// Author: Sushant Mane
// Course: CS-249 - Distributed Computing
// Project:  Chain Replication - Replicated Incremental HashTable Service
////////////////////////////////////////////////////////////////////////////

package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Replicated Incremental HashTable Service Server
 */
public class TailChainReplicaServer {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainReplicaServer.class);
    private Server server;
    private ZookeeperClient zk;
    private String ip;

    public TailChainReplicaServer(String zkAddress, String chainRoot, String ip, int port) {
        this.zk = new ZookeeperClient(zkAddress, chainRoot);
        this.ip = ip;
        this.server = ServerBuilder.forPort(port)
                .addService(new TailChainReplicaService(zk))
                .build();
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        zk.connect();       // connect to zookeeper service
        server.start();     // start gRPC server
        LOG.info("Server started on {}:{}", ip, server.getPort());
        System.out.println("Server started on " + ip + ":" + server.getPort());
        // add a shutdown hook to stop gRPC daemon threads gracefully on JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(TailChainReplicaServer.this::stop));
        // register this replica in zookeeper
        registerReplica();
        // wait for the gRPC server to become terminated
        server.awaitTermination();
    }

    private void registerReplica() throws KeeperException, InterruptedException {
        String data = ip + ":" + server.getPort();
        String path = zk.createEphZnode(zk.getSessionId(), data);
        LOG.info("Replica {} registered with zookeeper. ReplicaInfo: {}", path, data);
    }

    // initiate graceful shutdown of the gRPC server
    public void stop() {
        LOG.info("Server shutdown is in progress...");
        zk.closeConnection();       // disconnect zookeeper
        if (server != null) {
            server.shutdown();      // stop grpc server
        }
        System.out.println("Server has been stopped.");
    }
}
