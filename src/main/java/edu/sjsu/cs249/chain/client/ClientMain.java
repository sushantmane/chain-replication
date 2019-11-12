package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.util.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Scanner;

/**
 * Replicated Incremental HashTable Service Client
 */
public class ClientMain {

    private TailChainClient clientLib;
    private CommandLine cmd;

    private String getUsageStr() {
        String str = "\n"
                + "+--------------------------------------------------------+\n"
                + "|           Replicated Hashtable Service Client          |\n"
                + "| Usage: get key | inc key value | del key | quit | help |\n"
                + "+--------------------------------------------------------+\n";
        return str;
    }

    // interactive command prompt
    private void repl() throws KeeperException, InterruptedException {
        System.out.println(getUsageStr());
        Scanner scanner = new Scanner(System.in);
        String line;
        do {
            System.out.print("cli>>> ");
            line = scanner.nextLine();
            if (line.trim().length() == 0) {
                continue;
            }
            String[] input = line.split(" ");
            String op = input[0];
            String key;
            int val;
            switch (op) {
                case "get":
                    if (input.length < 2) {
                        System.err.println("Missing arguments");
                        break;
                    }
                    responseHandler("get", clientLib.get(input[1]));
                    break;
                case "inc":
                    if (input.length < 3) {
                        System.err.println("Missing arguments");
                        break;
                    }
                    key = input[1];
                    try {
                        val = Integer.parseInt(input[2]);
                    } catch (NumberFormatException e) {
                        // value not a number
                        System.err.println("Value should be an integer");
                        break;
                    }
                    responseHandler("inc", clientLib.increment(key, val));
                    break;
                case "del":
                    if (input.length < 2) {
                        System.err.println("Missing arguments");
                        break;
                    }
                    responseHandler("del", clientLib.delete(input[1]));
                    break;
                case "help":
                    System.out.println("Usage: get key | inc key value | del key | quit | help");
                    break;
                case "quit":
                    System.exit(0);
                default:
                    System.out.println("Operation not supported");
            }
        } while (true);
    }

    private ClientMain(CommandLine cmd) {
        this.cmd = cmd;
    }

    private void initClientLib() throws IOException, InterruptedException, KeeperException {
        String zkAddr = cmd.getOptionValue(ClientClaHandler.ORACLE);
        String root = cmd.getOptionValue(ClientClaHandler.ZROOT);
        int port = Integer.parseInt(cmd.getOptionValue(ClientClaHandler.PORT));
        String host = getHost();
        // *** start services ***
        clientLib = new TailChainClient(zkAddr, root, host, port); // client library object
//        clientLib.startTcServer();       // start TailClientService server
//        System.out.println("Connecting to zookeeper service...");
//        clientLib.connectToZk();
//        System.out.println("Zookeeper connection established");
        clientLib.init();
    }

    private String getHost() {
        String host;
        if (cmd.hasOption(ClientClaHandler.HOST)) {
            host = cmd.getOptionValue(ClientClaHandler.HOST);
        } else if (cmd.hasOption(ClientClaHandler.NIF)) {
            host = Utils.getHostIp4Addr(cmd.getOptionValue(ClientClaHandler.NIF));
        } else {
            host = Utils.getLocalhost();
        }
        return host;
    }

    private void responseHandler(String op, Response rsp) {
        if (rsp.getRc() == Response.Code.ECHNMTY) {
            System.err.println("Error: Chain Empty. Operation failed");
            return;
        }
        if (rsp.getRc() == Response.Code.EFAULT) {
            System.err.println("Error: Something went wrong. Please check logs.");
            return;
        }
        if (rsp.getRc() == Response.Code.EABORT) {
            System.err.println("Error: Local server is down. Restart client.");
            return;
        }
        if (rsp.getRc() == Response.Code.ENOKEY) {
            System.out.println("Key does not exist.");
            return;
        }

        switch (op) {
            case "get":
                System.out.println(rsp.getKey() + ": "+ rsp.getValue());
                break;
            case "del":
                System.out.println("Success: " + rsp.getKey() + " removed successfully");
                break;
            case "inc":
                System.out.println("Value has been updated");
                break;
            default:
                // do nothing
        }
    }

    private void run() throws KeeperException, InterruptedException {
        if (cmd.hasOption(ClientClaHandler.REPL)) {
            repl();
        }
        if (cmd.hasOption(ClientClaHandler.GET)) {
            String key = cmd.getOptionValue(ClientClaHandler.GET);
            responseHandler("get", clientLib.get(key));
        }
        if (cmd.hasOption(ClientClaHandler.INCR)) {
            String[] input = cmd.getOptionValues(ClientClaHandler.INCR);
            String key = input[0];
            int val = 0;
            try {
                val = Integer.parseInt(input[1]);
            } catch (NumberFormatException e) {
                System.err.println("Value should be an integer");
                System.exit(1);
            }
            responseHandler("inc", clientLib.increment(key, val));
        }
        if (cmd.hasOption(ClientClaHandler.DELETE)) {
            String key = cmd.getOptionValue(ClientClaHandler.DELETE);
            responseHandler("del", clientLib.delete(key));
        }
    }

    // entry point for client
    public static void main(String[] args) {
        CommandLine cmd = ClientClaHandler.parse(args);
        ClientMain app = new ClientMain(cmd);
        try {
            app.initClientLib();
            app.run();
        } catch (KeeperException | InterruptedException | IOException e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }
}
