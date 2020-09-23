package org.jlab.epsci.ersap.lake.ring.ft;

import com.lmax.disruptor.*;
import org.jlab.epsci.ersap.lake.ring.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

public class VTPSixStreamAggregator {

    /**
     * VTP ports
     */
    private int vtpPort1;
    private int vtpPort2;
    private int vtpPort3;
    private int vtpPort4;
    private int vtpPort5;
    private int vtpPort6;

    /**
     * Max ring items
     */
    private final int maxRingItems = 1024;

    /**
     * Ring buffers
     */
    RingBuffer<RingEvent> ringBuffer1;
    RingBuffer<RingEvent> ringBuffer2;
    RingBuffer<RingEvent> ringBuffer3;
    RingBuffer<RingEvent> ringBuffer4;
    RingBuffer<RingEvent> ringBuffer5;
    RingBuffer<RingEvent> ringBuffer6;
    RingBuffer<RingEvent> ringBuffer12;
    RingBuffer<RingEvent> ringBuffer34;
    RingBuffer<RingEvent> ringBuffer56;
    RingBuffer<RingEvent> ringBuffer1234;
    RingBuffer<RingEvent> ringBuffer123456;

    /**
     * Sequences
     */
    Sequence sequence1;
    Sequence sequence2;
    Sequence sequence3;
    Sequence sequence4;
    Sequence sequence5;
    Sequence sequence6;
    Sequence sequence12;
    Sequence sequence34;
    Sequence sequence56;
    Sequence sequence1234;
    Sequence sequence123456;

    /**
     * Sequence barriers
     */
    SequenceBarrier sequenceBarrier1;
    SequenceBarrier sequenceBarrier2;
    SequenceBarrier sequenceBarrier3;
    SequenceBarrier sequenceBarrier4;
    SequenceBarrier sequenceBarrier5;
    SequenceBarrier sequenceBarrier6;
    SequenceBarrier sequenceBarrier12;
    SequenceBarrier sequenceBarrier34;
    SequenceBarrier sequenceBarrier56;
    SequenceBarrier sequenceBarrier1234;
    SequenceBarrier sequenceBarrier123456;

    int runNumber;

    public VTPSixStreamAggregator(int vtpPort1, int vtpPort2, int vtpPort3,
                                  int vtpPort4, int vtpPort5, int vtpPort6,
                                  int runNumber) {
        this.vtpPort1 = vtpPort1;
        this.vtpPort2 = vtpPort2;
        this.vtpPort3 = vtpPort3;
        this.vtpPort4 = vtpPort4;
        this.vtpPort5 = vtpPort5;
        this.vtpPort6 = vtpPort6;

        this.runNumber = runNumber;

        ringBuffer1 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence1 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier1 = ringBuffer1.newBarrier();
        ringBuffer1.addGatingSequences(sequence1);

        ringBuffer2 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence2 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier2 = ringBuffer2.newBarrier();
        ringBuffer2.addGatingSequences(sequence2);

        ringBuffer3 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence3 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier3 = ringBuffer3.newBarrier();
        ringBuffer3.addGatingSequences(sequence3);

        ringBuffer4 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence4 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier4 = ringBuffer4.newBarrier();
        ringBuffer4.addGatingSequences(sequence4);

        ringBuffer5 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence5 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier5 = ringBuffer5.newBarrier();
        ringBuffer5.addGatingSequences(sequence5);

        ringBuffer6 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence6 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier6 = ringBuffer6.newBarrier();
        ringBuffer6.addGatingSequences(sequence6);

        ringBuffer12 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence12 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier12 = ringBuffer12.newBarrier();
        ringBuffer12.addGatingSequences(sequence12);

        ringBuffer34 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence34 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier34 = ringBuffer34.newBarrier();
        ringBuffer34.addGatingSequences(sequence34);

        ringBuffer56 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence56 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier56 = ringBuffer56.newBarrier();
        ringBuffer56.addGatingSequences(sequence56);

        ringBuffer1234 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence1234 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier1234 = ringBuffer1234.newBarrier();
        ringBuffer1234.addGatingSequences(sequence1234);

        ringBuffer123456 = createSingleProducer(new RingEventFactory(), maxRingItems,
                new LiteBlockingWaitStrategy());
        sequence123456 = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequenceBarrier123456 = ringBuffer123456.newBarrier();
        ringBuffer123456.addGatingSequences(sequence123456);

    }

    private void go() {
        Receiver receiver1 = new Receiver(vtpPort1, 1, ringBuffer1, 10);
        Receiver receiver2 = new Receiver(vtpPort2, 2, ringBuffer2, 10);
        Receiver receiver3 = new Receiver(vtpPort3, 3, ringBuffer3, 10);
        Receiver receiver4 = new Receiver(vtpPort4, 4, ringBuffer4, 10);
        Receiver receiver5 = new Receiver(vtpPort5, 5, ringBuffer5, 10);
        Receiver receiver6 = new Receiver(vtpPort6, 6, ringBuffer6, 10);

        Aggregator aggregator12 = new Aggregator(ringBuffer1, ringBuffer2, sequence1,
                sequence2, sequenceBarrier1, sequenceBarrier2, ringBuffer12);

        Aggregator aggregator34 = new Aggregator(ringBuffer3, ringBuffer4, sequence3,
                sequence4, sequenceBarrier3, sequenceBarrier4, ringBuffer34);

        Aggregator aggregator56 = new Aggregator(ringBuffer5, ringBuffer6, sequence5,
                sequence6, sequenceBarrier5, sequenceBarrier6, ringBuffer56);

        Aggregator aggregator1234 = new Aggregator(ringBuffer12, ringBuffer34, sequence12,
                sequence34, sequenceBarrier12, sequenceBarrier34, ringBuffer1234);

        Aggregator aggregator123456 = new Aggregator(ringBuffer1234, ringBuffer56, sequence1234,
                sequence56, sequenceBarrier1234, sequenceBarrier56, ringBuffer123456);

        Consumer consumer = new Consumer(ringBuffer123456, sequence123456, sequenceBarrier123456, runNumber);

        receiver1.start();
        receiver2.start();
        receiver3.start();
        receiver4.start();
        receiver5.start();
        receiver6.start();

        aggregator12.start();
        aggregator34.start();
        aggregator56.start();
        aggregator1234.start();
        aggregator123456.start();

        consumer.start();

    }

    public static void main(String[] args) {
        int port1 = Integer.parseInt(args[0]);
        int port2 = Integer.parseInt(args[1]);
        int port3 = Integer.parseInt(args[2]);
        int port4 = Integer.parseInt(args[3]);
        int port5 = Integer.parseInt(args[4]);
        int port6 = Integer.parseInt(args[5]);
        int run_number = Integer.parseInt(args[6]);

        new VTPSixStreamAggregator(port1, port2, port3, port4, port5, port6, run_number).go();
    }

}
