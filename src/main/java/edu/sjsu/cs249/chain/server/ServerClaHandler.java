package edu.sjsu.cs249.chain.server;

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
 * Server Command Line Argument Handler
 */
public class ServerClaHandler {

    static final String APPNAME = "ServerMain";
    static final String HELP = "h";
    static final String PORT = "p";
    static final String ZROOT = "r";
    static final String ORACLE = "z";
    static final String NIF = "nif";
    static final String HOST = "host";

    public static CommandLine parse(String[] args) {
        /**
         * Print help if -h flag is set.
         * This works only if -h is first cmd argument.
         */
        checkForHelp(args);
        Options options = getOptions();
        CommandLineParser cliParser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = cliParser.parse(options, args, true);
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
        return cli;
    }

    private static void printUsageAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        formatter.printUsage(pw, 80, APPNAME, getOptions());
        pw.flush();
        System.exit(1);
    }

    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(APPNAME, "\nWhere:", getOptions(), null, true);
        System.exit(0);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder(HELP)
                .desc("display this help and exit")
                .longOpt("help")
                .build());
        options.addOption(Option.builder(PORT)
                .desc("port on which server will bind")
                .hasArg()
                .longOpt("port")
                .required()
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
        OptionGroup hostOptions = new OptionGroup();
        hostOptions.addOption(Option.builder()
                .desc("IP on which server will bind")
                .hasArg()
                .longOpt(HOST)
                .argName("host-ip")
                .build());
        hostOptions.addOption(Option.builder()
                .desc("network interface on which server will bind")
                .hasArg()
                .longOpt(NIF)
                .argName("net-interface")
                .build());
        options.addOptionGroup(hostOptions);
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
