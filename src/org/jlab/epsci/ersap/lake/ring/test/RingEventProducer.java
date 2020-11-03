package org.jlab.epsci.ersap.lake.ring.test;

import com.lmax.disruptor.RingBuffer;
import org.jlab.epsci.ersap.lake.ring.RingEvent;

import java.math.BigInteger;

/**
 * Ring produced. Gets the buffer from the ring that
 * can hold the RingEvent object.
 * Fills it and publishes it on the ring.
 */
class RingEventProducer {

    private final RingBuffer<RingEvent> ringBuffer;
    private final int ringBufferId;

    RingEventProducer(RingBuffer<RingEvent> ringBuffer, int rbId) {
        this.ringBuffer = ringBuffer;
        this.ringBufferId = rbId;
    }

    void onData(BigInteger nRecord, int payloadLength, byte[] payload) {
        // Grab the next sequence
        long sequence = ringBuffer.next();
        try {
            // Get the entry in the Disruptor
            RingEvent event = ringBuffer.get(sequence);
            event.setStreamId(ringBufferId);
            event.setRecordNumber(nRecord);
            event.setPayload(payload);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
