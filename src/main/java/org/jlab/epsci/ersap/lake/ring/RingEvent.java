package org.jlab.epsci.ersap.lake.ring;

import java.math.BigInteger;

public class RingEvent {
    private int streamId;
    private BigInteger recordNumber;
    private byte[] payload;

    public int getStreamId() {
        return streamId;
    }

    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    public BigInteger getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(BigInteger recordNumber) {
        this.recordNumber = recordNumber;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
