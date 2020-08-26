package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.RingBuffer;

public class StreamEventProducer {

    private final RingBuffer<StreamEvent> ringBuffer;
    private final int ringBufferId;

    public StreamEventProducer(RingBuffer<StreamEvent> ringBuffer, int rbId) {
        this.ringBuffer = ringBuffer;
        this.ringBufferId = rbId;
    }

    public void onData(long nRecord, int payloadLength, byte[] payload) {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try {
            StreamEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
            event.setSatreamId(ringBufferId);
            event.setRecordNumber(nRecord);
            event.setPayloadLongth(payloadLength);
            event.setPayload(payload);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
