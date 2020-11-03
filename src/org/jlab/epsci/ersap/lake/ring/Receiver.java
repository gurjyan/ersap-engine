package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.RingBuffer;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Receives stream frames from a VTP and writes them to a RingBuffer
 *
 *  ___        __
 * |   |      /  \
 * |   | ---> \  /
 *  ---        --
 *
 */
public class Receiver extends Thread {


    /**
     * VTP data stream
     */
    private DataInputStream dataInputStream;

    /**
     * Stream ID
     */
    private int streamId;

    /**
     * Output ring
     */
    private RingBuffer<RingEvent> ringBuffer;

    /**
     * Current spot in the ring from which an item was claimed.
     */
    private final AtomicLong getSequence = new AtomicLong();

    /**
     * For statistics
     */
    private int statLoop;
    private int statPeriod;
    private double totalData;
    private int rate;
    private long missed_record;
    private long prev_rec_number;


    public Receiver(int vtpPort, int streamId, RingBuffer<RingEvent> ringBuffer, int statPeriod) {
        this.ringBuffer = ringBuffer;
        this.streamId = streamId;
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

    }

    /**
     * Get the next available item in ring buffer for writing data.
     *
     * @return next available item in ring buffer.
     * @throws InterruptedException if thread interrupted.
     */
    private RingEvent get() throws InterruptedException {

        getSequence.set(ringBuffer.next());
        RingEvent buf = ringBuffer.get(getSequence.get());
        return buf;
    }

     private void publish() {
        ringBuffer.publish(getSequence.get());
    }


    private void decodeVtpHeader(RingEvent evt) {
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

            BigInteger rcn = EUtil.toUnsignedBigInteger(record_number);
//                BigInteger tsc = EUtil.toUnsignedBigInteger(ts_sec);
//                BigInteger tsn = EUtil.toUnsignedBigInteger(ts_nsec);
//                System.out.println("DDD => "+streamId+":   "+ rcn +" "+tsc+" "+tsn);

            byte[] dataBuffer = new byte[payload_length];
            dataInputStream.readFully(dataBuffer);
            evt.setPayload(dataBuffer);
            evt.setRecordNumber(rcn);
            evt.setStreamId(streamId);

            // Collect statistics
            missed_record = missed_record + (record_number - (prev_rec_number + 1));
            prev_rec_number = record_number;
            totalData = totalData + (double) total_length / 1000.0;
            rate++;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            while (true) {
                // Get an empty item from ring
                RingEvent buf = get();

                decodeVtpHeader(buf);

                // Make the buffer available for consumers
                publish();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class PrintRates extends TimerTask {

        @Override
        public void run() {
            if (statLoop <= 0) {
                System.out.println("stream:" + streamId
                        + " event rate =" + rate / statPeriod
                        + " Hz.  data rate =" + totalData / statPeriod + " kB/s."
                        + " missed rate = " + missed_record / statPeriod + " Hz."
                );
                statLoop = statPeriod;
                rate = 0;
                totalData = 0;
                missed_record = 0;
            }
            statLoop--;
        }
    }

}
