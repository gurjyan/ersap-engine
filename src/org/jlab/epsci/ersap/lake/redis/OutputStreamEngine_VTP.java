package org.jlab.epsci.ersap.lake.redis;

import org.jlab.epsci.ersap.util.EUtil;
import org.jlab.epsci.ersap.util.NonBlockingQueue;
import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Multi-threaded data-lake output stream engine.
 * Consumes VTP stream-frames from the data-lake
 */
public class OutputStreamEngine_VTP implements Runnable {

    private final byte[] streamName;

    private final int threadPoolSize;

    private final int statLoopLimit;
    private int statLoop;
    private double totalData;
    private int rate;
    private int lakeReads;

    private final Jedis dataLake;

    private final NonBlockingQueue<byte[]> localQueue = new NonBlockingQueue<>(1000);

    /**
     * Data-lake output stream engine constructor.
     *
     * @param name           VTP stream name (e.g. detector/crate/section)
     * @param lakeHost       Data-lake host name
     * @param lakePort       Data-lake port number
     * @param threadPoolSize Thread pool size
     * @param statPeriod     The period to print statistics. Note that
     *                       statistics are measured every second by the
     *                       separate Timer thread.
     */
    public OutputStreamEngine_VTP(String name, String lakeHost, int lakePort,
                                  int threadPoolSize, int statPeriod) {
        EUtil.requireNonNull(name, "stream name");
        streamName = name.getBytes();
        EUtil.requireNonNull(lakeHost, "data-lake object");
        this.threadPoolSize = threadPoolSize;

        statLoopLimit = statPeriod;
        // Timer for measuring and printing statistics.
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);

        dataLake = new Jedis(lakeHost, lakePort);
        System.out.println("DataLake connection succeeded. ");
        System.out.println("DataLake ping - " + dataLake.ping());
    }

    @Override
    public void run() {
        // Threads to process data.
        for (int i = 0; i < threadPoolSize; i++) {
            new Thread(new Worker()).start();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while (true) {
            if (dataLake.isConnected()) {
                byte[] dataOfTheLake = dataLake.lpop(streamName);
                if (dataOfTheLake != null) {
                    localQueue.add(dataOfTheLake);
                    lakeReads++;
                }
            }
        }
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            while (true) {
                byte[] b = localQueue.poll();
                if (b != null) {
                    ByteBuffer sFrame = ByteBuffer.wrap(b);
                    sFrame.order(ByteOrder.LITTLE_ENDIAN);
                    // start decoding the frame here

                    totalData = totalData + (double) sFrame.limit() / 1000.0;
                    rate++;
                }
            }
        }
    }

    private class PrintRates extends TimerTask {
        @Override
        public void run() {
            if (statLoop <= 0) {
                System.out.println(new String(streamName) + ": event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s."
                        + " lake reads =" + lakeReads + " Hz."
                );
                statLoop = statLoopLimit;
            }
            rate = 0;
            lakeReads = 0;
            totalData = 0;
            statLoop--;
        }
    }

}
