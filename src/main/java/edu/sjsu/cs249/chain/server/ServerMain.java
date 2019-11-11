package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.util.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ServerMain {

    // todo: refactor
    private static String getHost(CommandLine cli) {
        String host;
        if (cli.hasOption(ServerClaHandler.HOST)) {
            host = cli.getOptionValue(ServerClaHandler.HOST);
        } else if (cli.hasOption(ServerClaHandler.NIF)) {
            host = Utils.getHostIp4Addr(cli.getOptionValue(ServerClaHandler.NIF));
        } else {
            host = Utils.getLocalhost();
        }
        return host;
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        CommandLine cli = ServerClaHandler.parse(args);
        String zkAddr = cli.getOptionValue(ServerClaHandler.ORACLE);
        String root = cli.getOptionValue(ServerClaHandler.ZROOT);
        int port = Integer.parseInt(cli.getOptionValue(ServerClaHandler.PORT));
        String ip = getHost(cli);
        TailChainReplicaServer server = new TailChainReplicaServer(zkAddr, root, ip, port);
        server.start();
    }
}