package org.jlab.epsci.ersap.lake;

import org.jlab.epsci.ersap.EException;
import org.jlab.epsci.ersap.util.EUtil;
import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Multi-threaded data-lake output stream engine.
 * Consumes VTP stream-frames from the data-lake
 */
public class OutputStreamEngine_VTP implements Runnable {

    private final Jedis dataLake;
    private final byte[] streamName;

    private final ExecutorService dataLakeReaderThreadPool;

    private final int statLoopLimit;
    private int statLoop;
    private double totalData;
    private int rate;

    /**
     * Data-lake output stream engine constructor.
     *
     * @param name           VTP stream name (e.g. detector/crate/section)
     * @param lake           The data-lake connection object
     * @param threadPoolSize Thread pool size
     * @param statPeriod     The period to print statistics. Note that
     *                       statistics are measured every second by the
     *                       separate Timer thread.
     */
    public OutputStreamEngine_VTP(String name, Jedis lake,
                                  int threadPoolSize, int statPeriod) {
        EUtil.requireNonNull(name,"stream name");
        EUtil.requireNonNull(lake,"data-lake object");
        streamName = name.getBytes();
        dataLake = lake;
        statLoopLimit = statPeriod;
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize parameter must be larger than 0.");
        }
        // Timer for measuring and printing statistics.
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);
        // Thread pools for writing frames into the data-lake.
        dataLakeReaderThreadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void run() {
        try {
            if (dataLake.isConnected()) {
                Future<byte[]> dataOfTheLake = dataLakeReaderThreadPool.submit(() -> dataLake.lpop(streamName));
                ByteBuffer sFrame = ByteBuffer.wrap(dataOfTheLake.get());
                sFrame.order(ByteOrder.LITTLE_ENDIAN);
                totalData = totalData + (double) sFrame.limit() / 1000.0;
                rate++;

                // start decoding the frame here

            } else throw new EException("Data-lake communication error.");
        } catch (InterruptedException | ExecutionException | EException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    private class PrintRates extends TimerTask {
        @Override
        public void run() {
            if (statLoop <= 0) {
                System.out.println(Arrays.toString(streamName) + ": event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s.");
                statLoop = statLoopLimit;
            }
            rate = 0;
            totalData = 0;
            statLoop--;
        }
    }

}
