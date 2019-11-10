// Author: Sushant Mane
// Course: CS-249 - Distributed Computing
// Project:  Chain Replication - Replicated Incremental HashTable Service
////////////////////////////////////////////////////////////////////////////

package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.util.Utils;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.CreateMode;
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
    private int port;
    private ZookeeperClient zk;
    private TailChainReplicaService replService;

    public TailChainReplicaServer(String zkAddress, String chainRoot, int port) {
        this.zk = new ZookeeperClient(zkAddress, chainRoot);
        this.port = port;
        this.replService = new TailChainReplicaService(zk);
        this.server = ServerBuilder.forPort(port).addService(this.replService).build();
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        // connect to zookeeper service
        zk.connect();
        // start gRPC server
        server.start();
        LOG.info("Server started successfully on the port: {}", port);
        // register a shutdown hook to stop gRPC daemon threads gracefully on JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Server shutdown is in progress...");
            TailChainReplicaServer.this.stop();
            LOG.info("Server has been stopped successfully.");
        }));
        // register this replica in zookeeper
        String chainRoot = "/tail-chain";
        registerReplica();
        // wait for the gRPC server to become terminated
        server.awaitTermination();
    }

    // initiate graceful shutdown of the gRPC server
    private void stop() {
        // disconnect zookeeper
        zk.closeConnection();
        // stop grpc server
        if (server != null) {
            server.shutdown();
        }
    }

    // should be handled in another class
    private void registerReplica() throws KeeperException, InterruptedException {
        String data = Utils.getLocalhost() + ":" + port;
        String name = zk.getRoot() + "/" + Utils.getHexSid(zk.getSessionId());
        LOG.info("Registering replica with zookeeper - name: {} data: {}", name, data);
        String path = zk.create(name, data, CreateMode.EPHEMERAL);
        LOG.info("Replica {} registered successfully with zookeeper.", path);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String zkAddress = "192.168.56.111:9999";
        String chainRoot = "/tail-chain";
        int port = 5144;
        TailChainReplicaServer server = new TailChainReplicaServer(zkAddress, chainRoot, port);
        server.start();
    }
}
