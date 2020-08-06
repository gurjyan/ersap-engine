package org.jlab.epsci.ersap.lake;

import org.jlab.epsci.ersap.util.EUtil;
import org.jlab.epsci.ersap.util.NonBlockingQueue;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Multi-threaded data-lake input stream engine.
 * Receives VTP stream and injects stream-frames
 * into the data-lake.
 */
public class InputStreamEngine_VTP implements Runnable {

    private DataInputStream dataInputStream;

    private String dlHost;
    private int streamHighWaterMark;
    private final byte[] streamName;
    private long inLakeFrameNumbers;

    private int threadPoolSize;

    private final int statLoopLimit;
    private int statLoop;
    private double totalData;
    private int rate;
    private int vtpReads;
    private int lakeWrites;


    private final NonBlockingQueue<byte[]> localQueue = new NonBlockingQueue<>(1000);

    /**
     * Data-lake input stream engine constructor.
     * Parses total_size of the VTP frame and
     * reads the frame in its entirety.
     * Periodically prints receiver event rate
     * and data rate.
     *
     * @param name       VTP stream name (e.g. detector/crate/section)
     * @param port       The port number where VTP sends stream frames
     * @param statPeriod the period to print statistics. Note that
     *                   statistics are measured every second by the
     *                   separate Timer thread.
     */
    public InputStreamEngine_VTP(String name, int port, int statPeriod) {
        EUtil.requireNonNull(name, "stream name");
        if (port <= 0) {
            throw new IllegalArgumentException("Illegal port number.");
        }
        streamName = name.getBytes();
        statLoopLimit = statPeriod;
        // Timer for measuring and printing statistics.
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);
        // connecting to the VTP stream source
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server is listening on port " + port);
            Socket socket = serverSocket.accept();
            System.out.println("VTP client connected");
            InputStream input = socket.getInputStream();
            dataInputStream = new DataInputStream(new BufferedInputStream(input));
            dataInputStream.readInt();
            dataInputStream.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Data-lake input stream engine constructor.
     * Parses total_size of the VTP frame and
     * reads the frame in its entirety.
     * Writes frames into the data-lake in the
     * form of linked list of frames. Note that
     * in case the number of frames in the data-
     * lake is larger than high-water-mark stream
     * frames will not be recorded in the data-
     * lake, and will be dropped.
     * Periodically prints receiver event rate
     * and data rate.
     *
     * @param name           VTP stream name (e.g. detector/crate/section)
     * @param port           The port number where VTP sends stream frames
     * @param lakeHost       The host of the data-lake
     * @param highWaterMark  Max number of frames in the data-lake.
     * @param threadPoolSize Thread pool size
     * @param statPeriod     The period to print statistics. Note that
     *                       statistics are measured every second by the
     *                       separate Timer thread.
     */
    public InputStreamEngine_VTP(String name, int port,
                                 String lakeHost, int highWaterMark,
                                 int threadPoolSize, int statPeriod) {
        EUtil.requireNonNull(name, "stream name");
        streamName = name.getBytes();
        if (port <= 0) {
            throw new IllegalArgumentException("Illegal port number.");
        }
        statLoopLimit = statPeriod;
        EUtil.requireNonNull(lakeHost, "data-lake object");
        dlHost = lakeHost;

        if (highWaterMark <= 0) {
            throw new IllegalArgumentException("highWaterMark parameter must be larger than 0.");
        } else {
            streamHighWaterMark = highWaterMark;
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize parameter must be larger than 0.");
        }
        this.threadPoolSize = threadPoolSize;

        // Timer for measuring and printing statistics.
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);
        // Connecting to the VTP stream source
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server is listening on port " + port);
            Socket socket = serverSocket.accept();
            System.out.println("VTP client connected");
            InputStream input = socket.getInputStream();
            dataInputStream = new DataInputStream(new BufferedInputStream(input));
            dataInputStream.readInt();
            dataInputStream.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < threadPoolSize; i++) {
            new Thread(new Worker()).start();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (true) {
            try {
                // first word is source ID
                dataInputStream.readInt();
                int total_length = Integer.reverseBytes(dataInputStream.readInt());
                // note that we already read 2 words: source and total_length
                byte[] dataBuffer = new byte[total_length - (2 * 4)];
                dataInputStream.readFully(dataBuffer);
                // write to the non-blocking queue
                localQueue.add(dataBuffer);
                // collect statistics
                totalData = totalData + (double) total_length / 1000.0;
                rate++;
                vtpReads++;
            } catch (IOException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }
    }

    private class Worker implements Runnable {
        private Jedis dataLake;
        public Worker() {
            dataLake = new Jedis(dlHost);
            System.out.println("DataLake connection succeeded. ");
            System.out.println("DataLake ping - " + dataLake.ping());
        }

        @Override
        public void run() {
            while (true) {
                // write to the lake
                if (streamHighWaterMark > 0) {
                    if (dataLake.isConnected()) {
                        inLakeFrameNumbers = dataLake.llen(streamName);
                        if (inLakeFrameNumbers < streamHighWaterMark) {
                            // get buffer from the non-blocking queue
                            byte[] b = localQueue.poll();
                            if (b != null) {
                                dataLake.lpush(streamName, b);
                                dataLake.lpop(streamName);
                                lakeWrites++;
                            }
                        }
                    } else {
                        System.out.println("Error: Not connected to the data-lake.");
                    }
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
                        + " lake frames = " + inLakeFrameNumbers
                        + " VTP reads = " + vtpReads
                        + " lake writes = " + lakeWrites
                );
                statLoop = statLoopLimit;
            }
            rate = 0;
            totalData = 0;
            vtpReads = 0;
            lakeWrites = 0;
            statLoop--;
        }
    }

}
