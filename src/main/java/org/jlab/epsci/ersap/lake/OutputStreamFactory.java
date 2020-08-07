package org.jlab.epsci.ersap.lake;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jlab.epsci.ersap.EException;
import org.jlab.epsci.ersap.util.OptUtil;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

public class OutputStreamFactory {
    private final OptionSpec<String> streamNamePrefix;
    private final OptionSpec<Integer> initStreamPortVtp;
    private final OptionSpec<Integer> numberOfStreams;
    private final OptionSpec<Integer> statPeriod;
    private final OptionSpec<String> dataLakeHost;
    private final OptionSpec<Integer> threadPoolSize;
    private final OptionParser parser;
    private OptionSet options;

    public OutputStreamFactory() {
        parser = new OptionParser();
        streamNamePrefix = parser.accepts("n")
                .withRequiredArg();
        initStreamPortVtp = parser.accepts("p")
                .withRequiredArg()
                .ofType(Integer.class);
        numberOfStreams = parser.accepts("m")
                .withRequiredArg()
                .ofType(Integer.class);
        statPeriod = parser.accepts("s")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(10);
        dataLakeHost = parser.accepts("l")
                .withRequiredArg();
        threadPoolSize = parser.accepts("t")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(2);
        parser.acceptsAll(Arrays.asList("h", "help")).forHelp();
    }

    public boolean hasHelp() {
        return options.has("help");
    }

    public boolean hasLake() {
        return options.has(dataLakeHost);
    }

    public void parse(String[] args) throws EException {
        try {
            options = parser.parse(args);
            if (hasHelp()) {
                return;
            }
            int numArgs = args.length;
            if (numArgs == 0) {
                throw new EException("missing arguments");
            }
            if (!hasLake()) {
                throw new EException("Data Lake host is not defined/");
            }
            if (!(numArgs == 12)) {
                throw new EException("invalid number of arguments");
            }
        } catch (OptionException e) {
            throw new EException(e.getMessage());
        }
    }

    public String usage() {
        String wrapper = "output-stream-factory";
        return String.format("usage: %s [options] ", wrapper)
                + String.format("%n%n  Options:%n")
                + OptUtil.optionHelp("-n",
                "VTP stream name prefix.")
                + OptUtil.optionHelp("-p",
                "Initial stream listening port number.")
                + OptUtil.optionHelp("-m",
                "Number of VTP streams.")
                + OptUtil.optionHelp("-s",
                "Period for printing statistics in second.")
                + OptUtil.optionHelp("-l",
                "Data-lake host name.")
                + OptUtil.optionHelp("-t",
                "Single stream worker pool size.");
    }

    public static void main(String[] args) {
        OutputStreamFactory factory = new OutputStreamFactory();
        try {
            factory.parse(args);
            if (factory.hasHelp()) {
                System.out.println(factory.usage());
                System.exit(0);
            }
            int vtpPort = factory.options.valueOf(factory.initStreamPortVtp);
            // start input stream engines
            Jedis lake = new Jedis(factory.options.valueOf(factory.dataLakeHost));
            System.out.println("DataLake connection succeeded. ");
            System.out.println("DataLake ping - " + lake.ping());

            for (int i = 0; i < factory.options.valueOf(factory.numberOfStreams); i++) {
                OutputStreamEngine_VTP engine = new OutputStreamEngine_VTP(
                        factory.options.valueOf(factory.streamNamePrefix) + vtpPort,
                        lake,
                        factory.options.valueOf(factory.threadPoolSize),
                        factory.options.valueOf(factory.statPeriod)
                );
                new Thread(engine).start();
                vtpPort++;
            }
        } catch (EException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(factory.usage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
