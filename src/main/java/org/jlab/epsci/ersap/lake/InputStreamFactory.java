package org.jlab.epsci.ersap.lake;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jlab.epsci.ersap.EException;
import org.jlab.epsci.ersap.util.OptUtil;
import redis.clients.jedis.Jedis;
import java.util.Arrays;

public class InputStreamFactory {
    private final OptionSpec<String> streamNamePrefix;
    private final OptionSpec<Integer> initStreamPortVtp;
    private final OptionSpec<Integer> numberOfStreams;
    private final OptionSpec<Integer> statPeriod;
    private final OptionSpec<String> dataLakeHost;
    private final OptionSpec<Integer> threadPoolSize;
    private final OptionSpec<Integer> highWaterMark;
    private final OptionParser parser;
    private OptionSet options;

    public InputStreamFactory() {
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
        highWaterMark = parser.accepts("w")
                .withRequiredArg()
                .ofType(Integer.class);
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
            int numArgs = options.nonOptionArguments().size();
            if (numArgs == 0) {
                throw new EException("missing arguments");
            }
            if (!(numArgs == 8 || numArgs == 14)) {
                throw new EException("invalid number of arguments");
            }
        } catch (OptionException e) {
            throw new EException(e.getMessage());
        }
    }

    public String usage() {
        String wrapper = "input-stream-factory";
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
                "Single stream worker pool size.")
                + OptUtil.optionHelp("-w",
                "Max number of stream-frames to be stored in the data-lake.");
    }

    public static void main(String[] args) {
        InputStreamFactory factory = new InputStreamFactory();
        try {
            factory.parse(args);
            if (factory.hasHelp()) {
                System.out.println(factory.usage());
                System.exit(0);
            }
            // start input stream engines
            if (factory.hasLake()) {
                Jedis lake = new Jedis(factory.options.valueOf(factory.dataLakeHost));
                System.out.println("DataLake connection succeeded. ");
                System.out.println("DataLake ping - " + lake.ping());

                for (int i = 0; i < factory.options.valueOf(factory.numberOfStreams); i++) {
                    InputStreamEngine_VTP engine = new InputStreamEngine_VTP(
                            factory.options.valueOf(factory.streamNamePrefix) + "_" + 6000 + i,
                            factory.options.valueOf(factory.initStreamPortVtp) + 1,
                            lake,
                            factory.options.valueOf(factory.highWaterMark),
                            factory.options.valueOf(factory.threadPoolSize),
                            factory.options.valueOf(factory.statPeriod)
                    );
                    new Thread(engine).start();
                }
            } else {
                for (int i = 0; i < factory.options.valueOf(factory.numberOfStreams); i++) {
                    InputStreamEngine_VTP engine = new InputStreamEngine_VTP(
                            factory.options.valueOf(factory.streamNamePrefix) + "_" + 6000 + i,
                            factory.options.valueOf(factory.initStreamPortVtp) + 1,
                            factory.options.valueOf(factory.statPeriod)
                    );
                    new Thread(engine).start();
                }
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
