package org.jlab.epsci.ersap.lake.ring;

public class StreamEvent {
    private int satreamId;
    private long recordNumber;
    private int payloadLongth;
    private byte[] payload;

    public int getSatreamId() {
        return satreamId;
    }

    public void setSatreamId(int satreamId) {
        this.satreamId = satreamId;
    }

    public long getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(long recordNumber) {
        this.recordNumber = recordNumber;
    }

    public int getPayloadLongth() {
        return payloadLongth;
    }

    public void setPayloadLongth(int payloadLongth) {
        this.payloadLongth = payloadLongth;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }


}
