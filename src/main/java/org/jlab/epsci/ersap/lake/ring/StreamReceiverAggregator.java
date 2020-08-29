package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

public class StreamReceiverAggregator {

    private DataInputStream dataInputStream;

    private final int statLoopLimit = 10; // 10 sec iterval
    private int statLoop;
    private double totalData;
    private int rate;
    private long prev_rec_number;
    private long missed_record;


    // Ring parameters
    private static final int bufferSize = 256;

    private static final int ringbufferId_c11  = 11;
    private static final  int ringbufferId_c12 = 12;

    private static final  int ringbufferId_c21 = 21;
    private static final  int ringbufferId_c22 = 22;

    private static final  int ringbufferId_c31 = 31;
    private static final  int ringbufferId_c32 = 32;


    public StreamReceiverAggregator(int vtpPort) {

        // create stream handler for 2 streams
        StreamEventHandler crateHandler_1 = new StreamEventHandler();
        StreamEventHandler crateHandler_2 = new StreamEventHandler();
        StreamEventHandler crateHandler_3 = new StreamEventHandler();

        // Timer for measuring and printing statistics.
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);


        // Connecting to the VTP stream source
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(vtpPort);
            System.out.println("Server is listening on port " + vtpPort);
            Socket socket = serverSocket.accept();
            System.out.println("VTP client connected");
            InputStream input = socket.getInputStream();
            dataInputStream = new DataInputStream(new BufferedInputStream(input));
            dataInputStream.readInt();
            dataInputStream.readInt();
        } catch (
                IOException e) {
            e.printStackTrace();
        }

        // Construct the Disruptor
        Disruptor<StreamEvent> disruptor = new Disruptor<>(new StreamEventFactory(), bufferSize, DaemonThreadFactory.INSTANCE);

        // Connect the handler
        disruptor.handleEventsWith(crateHandler_1);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<StreamEvent> ringBuffer_c11 = disruptor.getRingBuffer();


        StreamEventProducer producer = new StreamEventProducer(ringBuffer_c11, ringbufferId_c11);

        // start a loop for the the first vtp stream read the
        // data stream and put data into the ring
        while (true) {

            try {
                // decode header
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

//                byte[] dataBuffer = new byte[total_length - (13 * 4)]; // for soft_soure
                byte[] dataBuffer = new byte[payload_length];
                dataInputStream.readFully(dataBuffer);

                // fill event to write it into the ring
//                producer.onData(record_number,payload_length,dataBuffer);

                EUtil.decodeVtpPayload(dataBuffer);


                // collect statistics
                missed_record = missed_record + (record_number - (prev_rec_number + 1));
                prev_rec_number = record_number;
                totalData = totalData + (double) total_length / 1000.0;
                rate++;

            } catch (IOException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }

    }


    private class PrintRates extends TimerTask {

        @Override
        public void run() {
            if (statLoop <= 0) {
                System.out.println("event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s."
                        + " missed rate = " + missed_record + " Hz."
                );
                statLoop = statLoopLimit;
            }
            rate = 0;
            totalData = 0;
            missed_record = 0;
            statLoop--;
        }
    }

    public static void main(String[] args) {

        new StreamReceiverAggregator(Integer.parseInt(args[0]));
    }

}
