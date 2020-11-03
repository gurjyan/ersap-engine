package org.jlab.epsci.ersap.lake.ring.ft;

import com.lmax.disruptor.*;
import org.jlab.epsci.ersap.lake.ring.AdcHit;
import org.jlab.epsci.ersap.lake.ring.RingEvent;
import org.jlab.epsci.ersap.lake.ring.RingEventFactory;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is an example of how one might take 2 producers (one for each ring)
 * and have a consumer that reads one item from each ring and puts them both into
 * a third, output ring. That output ring has a consumer that looks at each item
 * in the output ring.
 * <p>
 * Note: there are more efficient ways of programming this.
 * This is the simplest and most straight forward.
 * If it works, we can think about making it faster.
 *
 * @author Carl Timmer
 */
public class TwoStreamAggregator {

    // FOR ONE CRATE:
    // Note: You may substitute your own Class for ByteBuffer so the ring can contain
    // whatever you want.

    int vtpPort1;
    int vtpPort2;
    /**
     * Number of streams in 1 crate.
     */
    int streamCount = 2;

    /**
     * Number of items in each ring buffer. Must be power of 2.
     */
    int crateRingItemCount = 1024;

    /**
     * 1 RingBuffer per stream.
     */
    RingBuffer<RingEvent>[] crateRingBuffers = new RingBuffer[streamCount];

    /**
     * 1 sequence per stream
     */
    Sequence[] crateSequences = new Sequence[streamCount];

    /**
     * 1 barrier per stream
     */
    SequenceBarrier[] crateBarriers = new SequenceBarrier[streamCount];

    /**
     * Track which sequence the aggregating consumer wants next from each of the crate rings.
     */
    long[] crateNextSequences = new long[streamCount];

    /**
     * Track which sequence is currently available from each of the crate rings.
     */
    long[] crateAvailableSequences = new long[streamCount];


    // OUTPUT RING FOR AGGREGATING CONSUMER

    /**
     * 1 output RingBuffer.
     */
    RingBuffer<RingEvent> outputRingBuffer;

    /**
     * 1 sequence for the output ring's consumer
     */
    Sequence outputSequence;

    /**
     * 1 barrier for output ring's consumer
     */
    SequenceBarrier outputBarrier;

    /**
     * Track which sequence the output consumer wants next from output ring.
     */
    long outputNextSequence;

    /**
     * Track which sequence is currently available from the output ring.
     */
    long outputAvailableSequence = -1;

    String outFileName;

    public TwoStreamAggregator(int port1, int port2, String fName) {

        vtpPort1 = port1;
        vtpPort2 = port2;
        outFileName = fName;

        //-----------------------
        // INPUT
        //-----------------------

        // For each stream ...
        for (int i = 0; i < streamCount; i++) {
            // Create a ring
            crateRingBuffers[i] = createSingleProducer(new RingEventFactory(), crateRingItemCount,
                    new LiteBlockingWaitStrategy());

            // Create a sequence
            crateSequences[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // Create a barrier in the ring
            crateBarriers[i] = crateRingBuffers[i].newBarrier();

            // Tell ring that after this sequence is "put back" by the consumer,
            // its associated ring item  will be
            // available for the producer to reuse (i.e. it's the last or gating consumer).
            crateRingBuffers[i].addGatingSequences(crateSequences[i]);

            // What sequence ring item do we want to get next?
            crateNextSequences[i] = crateSequences[i].get() + 1L;
        }

        // Initialize these values to indicate nothing is currently available from the ring
        Arrays.fill(crateAvailableSequences, -1L);

        //-----------------------
        // OUTPUT
        //-----------------------

        // Now create output ring
        outputRingBuffer = createSingleProducer(new RingEventFactory(), crateRingItemCount,
                new LiteBlockingWaitStrategy());

        outputSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        outputBarrier = outputRingBuffer.newBarrier();
        outputRingBuffer.addGatingSequences(outputSequence);
        outputNextSequence = outputSequence.get() + 1L;
    }


    /**
     * Run a setup with 2 crate producer threads, one crate consumer thread and one output ring
     * consumer thread.
     */
    public void go() {

        try {
            // Create 2 producers
            CrateProducer producer1 = new CrateProducer(vtpPort1, 0, 10);
            CrateProducer producer2 = new CrateProducer(vtpPort2, 1, 10);

            // Create one crate consumer
            CrateAggregatingConsumer crateConsumer = new CrateAggregatingConsumer();

            // Create one output ring consumer
            OutputRingConsumer outputConsumer = new OutputRingConsumer(outFileName);

            // Now get all these threads running
            outputConsumer.start();
            crateConsumer.start();
            producer1.start();
            producer2.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Thread to produce one stream in one crate.
     */
    class CrateProducer extends Thread {

        int vtpPort;

        private DataInputStream dataInputStream;

        private int statLoop;
        private int statPeriod;
        private double totalData;
        private int rate;
        private long missed_record;
        private long prev_rec_number;

        private int streamNum;
        /**
         * Current spot in ring from which an item was claimed.
         */
        long getSequence;


        CrateProducer(int vtpPort, int streamNumber, int statPeriod) {
            this.vtpPort = vtpPort;
            this.statPeriod = statPeriod;
            streamNum = streamNumber;

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
         * Get the next available item in ring buffer for writing/reading data.
         *
         * @return next available item in ring buffer.
         * @throws InterruptedException if thread interrupted.
         */
        public RingEvent get() throws InterruptedException {

            // Next available item for producer.
            getSequence = crateRingBuffers[streamNum].next();

            // Get object in that position (sequence) of ring
            RingEvent buf = crateRingBuffers[streamNum].get(getSequence);
            return buf;
        }

        /**
         * Used to tell the consumer that the ring buffer item gotten with this producer's
         * last call to {@link #get()} (and all previously gotten items) is ready for consumption.
         * To be used in after {@link #get()}.
         */
        public void publish() {
            crateRingBuffers[streamNum].publish(getSequence);
        }

        public void decodeVtpHeader(RingEvent evt) {
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
//                System.out.println("DDD => "+streamNum+":   "+ rcn +" "+tsc+" "+tsn);

                byte[] dataBuffer = new byte[payload_length];
                dataInputStream.readFully(dataBuffer);
                evt.setPayload(dataBuffer);
                evt.setRecordNumber(rcn);
                evt.setStreamId(streamCount);

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

                    // Do something with buffer here, like write data into it ...
                    // For now, just clear it.
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
                    System.out.println("stream:" + streamNum
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

    /**
     * Thread to consume from two streams in one crate and send (be a producer for) an output ring.
     */
    class CrateAggregatingConsumer extends Thread {

        /**
         * Array to store items obtained from both the crate (input) rings.
         */
        RingEvent[] inputItems = new RingEvent[streamCount];

        /**
         * Array to store items obtained from both the output ring.
         */
        RingEvent outputItem = new RingEvent();

        /**
         * Current spot in output ring from which an item was claimed.
         */
        long getOutSequence;

        private HashMap<BigInteger, byte[]> m1 = new HashMap<>();
        private HashMap<BigInteger, byte[]> m2 = new HashMap<>();


        CrateAggregatingConsumer() {
        }


        /**
         * Get the next available item from each crate ring buffer.
         * Do NOT call this multiple times in a row!
         * Be sure to call "put" before calling this again.
         *
         * @return next available item in ring buffer for getting data already written into.
         * @throws InterruptedException
         */
        public void get() throws InterruptedException {

            try {
                // Grab one ring item from each ring ...

                for (int i = 0; i < streamCount; i++) {
                    // Only wait for read-volatile-memory if necessary ...
                    if (crateAvailableSequences[i] < crateNextSequences[i]) {
                        // Note: the returned (available) sequence may be much larger than crateNextSequence[i]
                        // which means in the next iteration, we do NOT have to wait here.
                        crateAvailableSequences[i] = crateBarriers[i].waitFor(crateNextSequences[i]);
                    }

                    inputItems[i] = crateRingBuffers[i].get(crateNextSequences[i]);
                }
                BigInteger b1 = inputItems[0].getRecordNumber();
                BigInteger b2 = inputItems[1].getRecordNumber();

                m1.put(b1, inputItems[0].getPayload());
                m2.put(b2, inputItems[1].getPayload());

                byte[] aggregate = null;
                BigInteger aggRecNum = null;
                if (m1.containsKey(b1) && m2.containsKey(b1)) {
                    aggregate = EUtil.addByteArrays(m1.get(b1), m2.get(b1));
                    aggRecNum = b1;
                    m1.remove(b1);
                    m2.remove(b1);
                }
                if (m1.containsKey(b2) && m2.containsKey(b2)) {
                    aggregate = EUtil.addByteArrays(m1.get(b2), m2.get(b2));
                    aggRecNum = b2;
                    m1.remove(b2);
                    m2.remove(b2);
                }
                if (aggregate != null && aggRecNum != null) {
                    // Get next available slot in output ring (as producer)
                    getOutSequence = outputRingBuffer.next();
                    // Get object in that position (sequence or slot) of output ring
                    outputItem = outputRingBuffer.get(getOutSequence);
                    outputItem.setRecordNumber(aggRecNum);
                    outputItem.setPayload(aggregate);
                }

            } catch (final TimeoutException | AlertException ex) {
                // never happen since we don't use timeout wait strategy
                ex.printStackTrace();
            }
        }


        /**
         * This "consumer" is also a producer for the output ring.
         * So get items from the output ring and fill them with items claimed from the input rings.
         */
        public void put() throws InterruptedException {

            // Tell output ring, we're done with all items we took from it.
            // Make them available to output ring's consumer.
            //
            // By releasing getOutputSequence, we release that item and all
            // previously obtained items, so we only have to call this once
            // with the last sequence.
            outputRingBuffer.publish(getOutSequence);

            for (int i = 0; i < streamCount; i++) {
                // Tell input (crate) ring that we're done with the item we're consuming
                crateSequences[i].set(crateNextSequences[i]);

                // Go to next item to consume from input ring
                crateNextSequences[i]++;
            }
        }

        public void run() {
            try {
                while (true) {
                    // Get one item from each of a single crate's rings
                    get();

                    // Done with buffers so make them available for all rings again for reuse
                    put();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Thread to consume from output ring.
     */
    class OutputRingConsumer extends Thread {

        /**
         * Current spot in output ring from which an item was claimed.
         */
        long getOutSequence;
        String fileName;

        OutputRingConsumer(String fName) {
            fileName = fName;
        }


        /**
         * Get the next available item from outupt ring buffer.
         * Do NOT call this multiple times in a row!
         * Be sure to call "put" before calling this again.
         *
         * @return next available item in ring buffer.
         * @throws InterruptedException
         */
        public RingEvent get() throws InterruptedException {

            RingEvent item = null;

            try {
                if (outputAvailableSequence < outputNextSequence) {
                    outputAvailableSequence = outputBarrier.waitFor(outputNextSequence);
                }

                item = outputRingBuffer.get(outputNextSequence);
            } catch (final TimeoutException | AlertException ex) {
                // never happen since we don't use timeout wait strategy
                ex.printStackTrace();
            }

            return item;
        }


        /**
         * This "consumer" is also a producer for the output ring.
         * So get items from the output ring and fill them with items claimed from the input rings.
         */
        public void put() throws InterruptedException {

            // Tell input (crate) ring that we're done with the item we're consuming
            outputSequence.set(outputNextSequence);

            // Go to next item to consume on input ring
            outputNextSequence++;
        }

        private ArrayList<AdcHit> decodePayload(BigInteger frame_time_ns, byte[] payload) {
            ArrayList<AdcHit> res = new ArrayList<>();
            ByteBuffer bb = ByteBuffer.wrap(payload);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            int[] slot_ind = new int[8];
            int[] slot_len = new int[8];
            long tag = EUtil.getUnsignedInt(bb);
            if ((tag & 0x8FFF8000L) == 0x80000000L) {

                for (int jj = 0; jj < 8; jj++) {
                    slot_ind[jj] = EUtil.getUnsignedShort(bb);
                    slot_len[jj] = EUtil.getUnsignedShort(bb);
                }

                for (int i = 0; i < 8; i++) {
                    if (slot_len[i] > 0) {
                        bb.position(slot_ind[i] * 4);
                        int type = 0;
                        for (int j = 0; j < slot_len[i]; j++) {
                            int val = bb.getInt();
                            AdcHit hit = new AdcHit();

                            if ((val & 0x80000000) == 0x80000000) {
                                type = (val >> 15) & 0xFFFF;
                                hit.setCrate((val >> 8) & 0x007F);
                                hit.setSlot((val) & 0x001F);
                            } else if (type == 0x0001) /* FADC hit type */ {
                                hit.setQ((val) & 0x1FFF);
                                hit.setChannel((val >> 13) & 0x000F);
                                long v = ((val >> 17) & 0x3FFF) * 4;
                                BigInteger ht = BigInteger.valueOf(v);
                                hit.setTime(frame_time_ns.add(ht));
                                hit.setTime(ht);
                                res.add(hit);
                            }
                        }
                    }
                }
            } else {
                System.out.println("parser error: wrong tag");
                System.exit(0);
            }
            return res;
        }

        public void run() {
            try {
                FileOutputStream fos = new FileOutputStream(fileName, true);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                ObjectOutputStream oos = new ObjectOutputStream(bos);

                while (true) {
                    // Get an empty item from ring
                    RingEvent buf = get();
                    decodePayload(buf.getRecordNumber().multiply(EUtil.toUnsignedBigInteger(65536L)), buf.getPayload());

                    // decode aggregated payload
//                    ArrayList<AdcHit> o = decodePayload((buf.getRecordNumber().multiply(EUtil.toUnsignedBigInteger(65536L))),
//                            buf.getPayload());
//                    oos.writeObject(o);

                    // Make the buffer available for consumers
                    put();
                }

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        int port1 = Integer.parseInt(args[0]);
        int port2 = Integer.parseInt(args[1]);
        TwoStreamAggregator test = new TwoStreamAggregator(port1, port2, args[2]);
        test.go();
    }
}
