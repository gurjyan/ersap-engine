package org.jlab.epsci.ersap.lake.ring.test;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.jlab.epsci.ersap.lake.ring.RingEvent;
import org.jlab.epsci.ersap.lake.ring.RingEventFactory;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;


/**
 * Single VTP stream receiver. Parses the header and
 * passes it to the ring buffer with a handler thread
 * that will parse the payload.
 * Runs in its own thread.
 */
public class StreamReceiver implements Runnable {

    private DataInputStream dataInputStream;

    private int statLoop;
    private int statPeriod;
    private double totalData;
    private int rate;
    private long prev_rec_number;
    private long missed_record;

    private int ringId;
    private RingEventProducer producer;

    /**
     * @param vtpPort        A server port where VTP pushes the stream frames
     * @param ringBufferId   ringBuffer id. We have ringBuffer per stream so this is also stream ID
     * @param ringBufferSize Max number of stream frames ion the ringBuffer
     * @param crateHandler   Handler/Consumer on the ring
     * @param statPeriod     Period in seconds, after which stats will be printed on stdIO
     */
    StreamReceiver(int vtpPort,
                   int ringBufferId, int ringBufferSize, RingBufferHandler crateHandler,
                   int statPeriod) {

        this.ringId = ringBufferId;
        this.statPeriod = statPeriod;

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

        // Ring Buffer setup
        // Construct the Disruptor
        Disruptor<RingEvent> disruptor =
                new Disruptor<>(new RingEventFactory(), ringBufferSize, DaemonThreadFactory.INSTANCE);
        // Connect the handler
        disruptor.handleEventsWith(crateHandler);
        // Start the Disruptor, starts all threads running
        disruptor.start();
        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<RingEvent> ringBuffer = disruptor.getRingBuffer();
        // Create a stream producer
        producer = new RingEventProducer(ringBuffer, ringBufferId);
    }

    @Override
    public void run() {
        // Start a loop to read the data_stream, parse and put data into the ring
        while (true) {
            try {
                // Decode header
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

                BigInteger rcn = EUtil.toUnsignedBigInteger(record_number);

                byte[] dataBuffer = new byte[payload_length];
                dataInputStream.readFully(dataBuffer);

                // fill event to write it into the ring
                producer.onData(rcn, payload_length, dataBuffer);

                // For testing purposes
//                 EUtil.decodeVtpPayload(dataBuffer);

                // Collect statistics
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
                System.out.println("stream:" + ringId
                        + "event rate =" + rate
                        + " Hz.  data rate =" + totalData + " kB/s."
                        + " missed rate = " + missed_record + " Hz."
                );
                statLoop = statPeriod;
            }
            rate = 0;
            totalData = 0;
            missed_record = 0;
            statLoop--;
        }
    }

}
