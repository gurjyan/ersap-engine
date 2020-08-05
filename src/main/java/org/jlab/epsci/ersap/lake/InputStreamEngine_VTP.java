package org.jlab.epsci.ersap.lake;

import org.jlab.epsci.ersap.EException;
import org.jlab.epsci.ersap.util.EUtil;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Multi-threaded data-lake input stream engine.
 * Receives VTP stream and injects stream-frames
 * into the data-lake.
 */
public class InputStreamEngine_VTP implements Runnable {

    private DataInputStream dataInputStream;

    private Jedis dataLake;
    private int streamHighWaterMark;
    private final byte[] streamName;
    private long inLakeFrameNumbers;

    private ExecutorService dataLakeWriterThreadPool;

    private final int statLoopLimit;
    private int statLoop;
    private double totalData;
    private int rate;

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
        EUtil.requireNonNull(name,"stream name");
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
     * @param lake           The data-lake connection object
     * @param highWaterMark  Max number of frames in the data-lake.
     * @param threadPoolSize Thread pool size
     * @param statPeriod     The period to print statistics. Note that
     *                       statistics are measured every second by the
     *                       separate Timer thread.
     */
    public InputStreamEngine_VTP(String name, int port,
                                 Jedis lake, int highWaterMark,
                                 int threadPoolSize, int statPeriod) {
        this(name, port, statPeriod);
        EUtil.requireNonNull(lake,"data-lake object");
        dataLake = lake;
        if (highWaterMark <= 0) {
            throw new IllegalArgumentException("highWaterMark parameter must be larger than 0.");
        } else {
            streamHighWaterMark = highWaterMark;
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize parameter must be larger than 0.");
        }
        // Thread pools for writing frames into the data-lake.
        dataLakeWriterThreadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void run() {
        while (true) {
            try {
                // first word is source ID
                dataInputStream.readInt();
                int total_length = Integer.reverseBytes(dataInputStream.readInt());
                // note that we already read 2 words: source and total_length
                byte[] dataBuffer = new byte[total_length - (2 * 4)];
                dataInputStream.readFully(dataBuffer);
                // write to the lake
                if (streamHighWaterMark > 0) {
                    if (dataLake.isConnected()) {
                        inLakeFrameNumbers = dataLake.llen(streamName);
                        if (inLakeFrameNumbers < streamHighWaterMark)
                            dataLakeWriterThreadPool.submit(() -> dataLake.lpush(streamName, dataBuffer));
                    } else {
                        throw new EException("Data-lake communication error.");
                    }
                }
                // collect statistics
                totalData = totalData + (double) total_length / 1000.0;
                rate++;
            } catch (IOException | EException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }
    }

    private class PrintRates extends TimerTask {
        @Override
        public void run() {
            if (statLoop <= 0) {
                System.out.println(Arrays.toString(streamName) + ": event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s."
                        + " lake frames = " + inLakeFrameNumbers);
                statLoop = statLoopLimit;
            }
            rate = 0;
            totalData = 0;
            statLoop--;
        }
    }

}
