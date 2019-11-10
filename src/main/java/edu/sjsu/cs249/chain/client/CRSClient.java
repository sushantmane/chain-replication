package edu.sjsu.cs249.chain.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;

/**
 * Replicated Incremental HashTable Service Client
 */
public class CRSClient {

    private static final String APPNAME = "CRSClient";
    private static final String HELP = "h";
    private static final String PORT = "p";
    private static final String ZROOT = "r";
    private static final String INCR = "inc";
    private static final String GET = "get";
    private static final String DELETE = "del";
    private static final String ORACLE = "z";

    public static void main(String[] args) {

        /**
         * Print help if -h flag is set.
         * This works only if -h is first cmd argument.
         */
        checkForHelp(args);
        Options options = getOptions();
        CommandLineParser cliParser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = cliParser.parse(options, args, false);
            /**
             *  This check works only if all required args are present and -h flag is set.
             *  If some required args missing then it fails to print help,
             *  instead it prints error 'missing required options' along with usage.
             */
            if (cli.hasOption(HELP)) {
                printHelpAndExit();
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsageAndExit();
        }

        for (Option option : cli.getOptions()) {
            System.out.print(option.getLongOpt() + " " + option.getValue());
            if (option.getValues().length > 1) {
                System.out.print(" " + option.getValue(1));
            }
            System.out.println();
        }
    }

    private static void printUsageAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        formatter.printUsage(pw, 120, APPNAME, getOptions());
        pw.flush();
        System.exit(1);
    }

    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(APPNAME, getOptions(), true);
        System.exit(0);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder(HELP)
                .desc("display this help and exit")
                .longOpt("help")
                .build());
        options.addOption(Option.builder(PORT)
                .desc("client will listen on this port for messages from the tail replica")
                .hasArg()
                .longOpt("port")
                .argName("port-number")
                .build());
        options.addOption(Option.builder(ORACLE)
                .desc("zookeeper server address e.g. 192.168.56.111:5144")
                .longOpt("oracle")
                .hasArg()
                .required()
                .argName("connect-string")
                .build());
        options.addOption(Option.builder(ZROOT)
                .desc("parent znode for the chain")
                .longOpt("zroot")
                .required()
                .hasArg()
                .argName("root-znode")
                .build());

        OptionGroup group = new OptionGroup();
        group.addOption(Option.builder()
                .desc("increment the value of given key by specified value")
                .longOpt(INCR)
                .argName("key value")
                .numberOfArgs(2)
                .valueSeparator(' ')
                .build());
        group.addOption(Option.builder()
                .desc("get the value of given key")
                .longOpt(GET)
                .argName("key")
                .numberOfArgs(1)
                .build());
        group.addOption(Option.builder()
                .desc("remove the key from hashtable")
                .longOpt(DELETE)
                .argName("key")
                .numberOfArgs(1)
                .build());
        group.setRequired(true);
        options.addOptionGroup(group);
        return options;
    }

    /**
     * Need to add this check as DefaultParser throws an exception
     * if required cmd arguments are not provided and thus preventing
     * the use of just --help flag
     */
    private static void checkForHelp(String[] args) {
        Options ops = new Options();
        ops.addOption(Option.builder(HELP)
                .desc("display this help and exit")
                .longOpt("help")
                .required(false)
                .build());
        try {
            CommandLine line = new DefaultParser().parse(ops, args, true);
            if (line.hasOption(HELP)) {
                printHelpAndExit();
            }
        } catch (ParseException e) {
            // ignore other options for now
        }
    }
}
