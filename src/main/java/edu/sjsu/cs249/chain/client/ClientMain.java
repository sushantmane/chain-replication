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

    private TailChainClient client;
    private CommandLine cli;

    private String getUsageStr() {
        String str = "\n"
                + "+--------------------------------------------------------+\n"
                + "|           Replicated Hashtable Service Client          |\n"
                + "| Usage: get key | inc key value | del key | quit | help |\n"
                + "+--------------------------------------------------------+\n";
        return str;
    }

    private void repl() {
        System.out.println(getUsageStr());
        Scanner scanner = new Scanner(System.in);
        String line;
        do {
            System.out.print("cli>>> ");
            line = scanner.nextLine();
            if (line.trim().length() == 0) {
                continue;
            }
            String input[] = line.split(" ");
            String op = input[0];
            String key;
            int val;
            switch (op) {
                case "get":
                    if (input.length < 2) {
                        System.err.println("Missing arguments");
                        break;
                    }
                    key = input[1];
                    // todo: call get api
                    val = 2812;
                    System.out.println(val);
                    // handle tail change
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
                    // todo: call inc api
                    System.out.println("status: " + "ok");
                    // handle head change case
                    break;
                case "del":
                    if (input.length < 2) {
                        System.err.println("Missing arguments");
                        break;
                    }
                    key = input[1];
                    System.out.println("key deleted");
                    // handle head change case
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

    private ClientMain(CommandLine cli) throws IOException, InterruptedException {
        String zkAddr = cli.getOptionValue(ClientClaHandler.ORACLE);
        String root = cli.getOptionValue(ClientClaHandler.ZROOT);
        int port = Integer.parseInt(cli.getOptionValue(ClientClaHandler.PORT));
        String host = getHost(cli);
        client = new TailChainClient(zkAddr, root, host, port);
        System.out.println("Connecting to zookeeper service...");
        client.connectToZk();
        System.out.println("Connection established.");
    }

    // todo: refactor
    private static String getHost(CommandLine cli) {
        String host;
        if (cli.hasOption(ClientClaHandler.HOST)) {
            host = cli.getOptionValue(ClientClaHandler.HOST);
        } else if (cli.hasOption(ClientClaHandler.NIF)) {
            host = Utils.getHostIp4Addr(cli.getOptionValue(ClientClaHandler.NIF));
        } else {
            host = Utils.getLocalhost();
        }
        return host;
    }

    private void run(CommandLine cli) throws KeeperException, InterruptedException {
        if (cli.hasOption(ClientClaHandler.REPL)) {
            repl();
        } else if (cli.hasOption(ClientClaHandler.GET)) {
            String key = cli.getOptionValue(ClientClaHandler.GET);
            responseHandler("get", client.get(key));
        } else if (cli.hasOption(ClientClaHandler.INCR)) {
            String[] input = cli.getOptionValues(ClientClaHandler.INCR);
            String key = input[0];
            int val = 0;
            try {
                val = Integer.parseInt(input[1]);
            } catch (NumberFormatException e) {
                System.err.println("Value should be an integer");
                System.exit(1);
            }
            responseHandler("inc", client.increment(key, val));
        } else if (cli.hasOption(ClientClaHandler.DELETE)) {
            String key = cli.getOptionValue(ClientClaHandler.DELETE);
            responseHandler("del", client.delete(key));
        }
    }

    private void responseHandler(String op, Response rsp) {
        if (rsp.getRc() == Response.Code.ECHNMTY) {
            System.err.println("Error: Chain Empty. Operation failed");
            return;
        }
        if (rsp.getRc() == Response.Code.EFAULT) {
            System.err.println("Error: Something went wrong");
            return;
        }
        if (rsp.getRc() == Response.Code.ENOKEY) {
            System.out.println("Key does not exist.");
            return;
        }

        switch (op) {
            case "get":
                System.out.println(rsp.getValue());
                break;
            case "del":
                System.out.println("Key does not exist.");
                break;
            case "inc":
                System.out.println("Value has been updated.");
                break;
            default:
                // do nothing
        }
    }

    // entry point for client
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        CommandLine cli = ClientClaHandler.parse(args);
        ClientMain main = new ClientMain(cli);
        main.run(cli);
    }
}
