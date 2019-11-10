package edu.sjsu.cs249.chain.client;

import org.apache.commons.cli.CommandLine;

import java.util.Scanner;

/**
 * Replicated Incremental HashTable Service Client
 */
public class CRSClient {

    public static String getUsageStr() {
        String str = "\n"
                + "+-------------------------------------------------+\n"
                + "|       Replicated Hashtable Service Client       |\n"
                + "| Usage: get key | inc key value | del key | quit |\n"
                + "+-------------------------------------------------+\n";
        return str;
    }

    private static void repl() {
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
                    val = Integer.parseInt(input[2]);
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
                case "quit":
                    System.exit(0);
                default:
                    System.out.println("Operation not supported");
            }
        } while (true);
    }

    public static void main(String[] args) {
        CommandLine cli = ClientCliHandler.parse(args);
        if (cli.hasOption(ClientCliHandler.REPL)) {
            repl();
        }
    }
}
