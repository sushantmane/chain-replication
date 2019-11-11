package edu.sjsu.cs249.chain.server;

import org.apache.commons.cli.CommandLine;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ServerMain {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        CommandLine cli = ServerClaHandler.parse(args);
        String zkAddr = cli.getOptionValue(ServerClaHandler.ORACLE);
        String root = cli.getOptionValue(ServerClaHandler.ZROOT);
        int port = Integer.parseInt(cli.getOptionValue(ServerClaHandler.PORT));
        TailChainReplicaServer server = new TailChainReplicaServer(zkAddr, root, port);
        server.start();
    }
}
