package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.*;
import org.jlab.epsci.ersap.util.EUtil;

import java.math.BigInteger;
import java.util.HashMap;


/**
 * __
 * /  \
 * \  /     ___       __
 * --  --> |   |     /  \
 * __      |   |-->  \  /
 * /  \ --> ---       --
 * \  /
 * --
 */
public class Aggregator extends Thread {

    /**
     * Maps for aggregation
     */
    private HashMap<BigInteger, byte[]> m1 = new HashMap<>();
    private HashMap<BigInteger, byte[]> m2 = new HashMap<>();

    /**
     * Current spot in output ring from which an item was claimed.
     */
    private long getOutSequence;


    /**
     * 1 RingBuffer per stream.
     */
    private RingBuffer<RingEvent> ringBuffer1;
    private RingBuffer<RingEvent> ringBuffer2;

    /**
     * 1 sequence per stream
     */
    private Sequence sequence1;
    private Sequence sequence2;

    /**
     * 1 barrier per stream
     */
    private SequenceBarrier barrier1;
    private SequenceBarrier barrier2;

    /**
     * Track which sequence the aggregating consumer wants next from each of the crate rings.
     */
    private long nextSequence1;
    private long nextSequence2;

    /**
     * Track which sequence is currently available from each of the crate rings.
     */
    private long availableSequence1;
    private long availableSequence2;

    /**
     * 1 output RingBuffer.
     */
    private RingBuffer<RingEvent> outputRingBuffer;


    public Aggregator(RingBuffer<RingEvent> ringBuffer1, RingBuffer<RingEvent> ringBuffer2,
               Sequence sequence1, Sequence sequence2,
               SequenceBarrier barrier1, SequenceBarrier barrier2,
               RingBuffer<RingEvent> outputRingBuffer
    ) {

        this.ringBuffer1 = ringBuffer1;
        this.ringBuffer2 = ringBuffer2;
        this.sequence1 = sequence1;
        this.sequence2 = sequence2;
        this.barrier1 = barrier1;
        this.barrier2 = barrier2;
        this.outputRingBuffer = outputRingBuffer;

        ringBuffer1.addGatingSequences(sequence1);
        ringBuffer2.addGatingSequences(sequence2);

        nextSequence1 = sequence1.get() + 1L;
        nextSequence2 = sequence2.get() + 1L;

        availableSequence1 = -1L;
        availableSequence2 = -1L;
    }


    public void get() throws InterruptedException {

        try {
            if (availableSequence1 < nextSequence1) {
                availableSequence1 = barrier1.waitFor(nextSequence1);
            }
            RingEvent inputItem1 = ringBuffer1.get(nextSequence1);

            if (availableSequence2 < nextSequence2) {
                availableSequence2 = barrier2.waitFor(nextSequence2);
            }
            RingEvent inputItem2 = ringBuffer2.get(nextSequence2);

            BigInteger b1 = inputItem1.getRecordNumber();
            BigInteger b2 = inputItem2.getRecordNumber();

            m1.put(b1, inputItem1.getPayload());
            m2.put(b2, inputItem2.getPayload());

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
                getOutSequence = outputRingBuffer.next();
                RingEvent outputItem = outputRingBuffer.get(getOutSequence);
                outputItem.setRecordNumber(aggRecNum);
                outputItem.setPayload(aggregate);
            }

        } catch (final TimeoutException | AlertException ex) {
            ex.printStackTrace();
        }
    }


    /**
     * This "consumer" is also a producer for the output ring.
     * So get items from the output ring and fill them with items claimed from the input rings.
     */
    private void put() throws InterruptedException {

        outputRingBuffer.publish(getOutSequence);

        sequence1.set(nextSequence1);
        nextSequence1++;

        sequence2.set(nextSequence2);
        nextSequence2++;

    }

    public void run() {
        try {
            while (true) {
                get();
                put();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
