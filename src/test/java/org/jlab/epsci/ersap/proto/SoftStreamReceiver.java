package org.jlab.epsci.ersap.proto;

import org.jlab.epsci.ersap.util.EUtil;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SoftStreamReceiver {

    private DataInputStream dataInputStream;
    private static BigInteger FRAME_TIME;
    private static final long ft_const = 65536L;
    private static int streamSourcePort = 6000;
    private static String streamSourceName = "s1";

    private volatile double totalData;
    private int loop = 10;
    private int rate;

    private long prev_rec_number;
    private long missed_record;

    private static boolean pushDL = false;
    private final byte[] key;
    ExecutorService redisWriterService;
    ExecutorService redisReaderService;

    // redis... =================================
    private final Jedis dataLake;

    public SoftStreamReceiver() {

        key = streamSourceName.getBytes();
        // Timer for printing statistics
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);

        // Thread pools for Redis writing and reading
        redisWriterService = Executors.newFixedThreadPool(4);
        redisReaderService = Executors.newFixedThreadPool(4);

        // redis.. ====================================
        dataLake = new Jedis("localhost");
        System.out.println("DataLake connection succeeded. ");
        System.out.println("DataLake ping - " + dataLake.ping());

        FRAME_TIME = EUtil.toUnsignedBigInteger(ft_const);
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(streamSourcePort);
            System.out.println("Server is listening on port " + streamSourcePort);
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

    private void readSoftStream() {
        try {
            int source_id = Integer.reverseBytes(dataInputStream.readInt());
            int total_length = Integer.reverseBytes(dataInputStream.readInt());
            int payload_length = Integer.reverseBytes(dataInputStream.readInt());
            int compressed_length = Integer.reverseBytes(dataInputStream.readInt());
            int magic = Integer.reverseBytes(dataInputStream.readInt());

            int format_version = Integer.reverseBytes(dataInputStream.readInt());
            int flags = Integer.reverseBytes(dataInputStream.readInt());
            long record_number = EUtil.llSwap(Long.reverseBytes(dataInputStream.readLong()));
            long ts_sec = EUtil.llSwap(Long.reverseBytes(dataInputStream.readLong()));
            long ts_nsec = EUtil.llSwap(Long.reverseBytes(dataInputStream.readLong()));

            missed_record = missed_record + (record_number - (prev_rec_number + 1));
            prev_rec_number = record_number;

            byte[] dataBuffer = new byte[total_length - (13 * 4)];
            dataInputStream.readFully(dataBuffer);

            if(pushDL) {
                redisWriterService.submit( ()-> dataLake.lpush(key, dataBuffer) );
                Future<byte[]> dataOfTheLake = redisReaderService.submit ( ()-> dataLake.lpop(key) );
//                dataLake.lpush(key, dataBuffer);
//                dataLake.lpop(key);
            }

            totalData = totalData + (double) total_length / 1000.0;
            rate++;

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private class PrintRates extends TimerTask {

        @Override
        public void run() {
            if (loop <= 0) {
                System.out.println("event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s." +
                        " missed rate = " + missed_record + " Hz.");
                loop = 10;
            }
            rate = 0;
            missed_record = 0;
            totalData = 0;
            loop--;
        }
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            if (args[0].equals("-dl")) {
                pushDL = true;
            }
        } else if (args.length == 2){
            if (args[0].equals("-dl")) {
                pushDL = true;
            }
            streamSourcePort = Integer.parseInt(args[1]);
        } else if (args.length == 3){
            if (args[0].equals("-dl")) {
                pushDL = true;
            }
            streamSourcePort = Integer.parseInt(args[1]);
            streamSourceName = args[2];
        }
        SoftStreamReceiver sr = new SoftStreamReceiver();
        while (true) sr.readSoftStream();
    }
}

