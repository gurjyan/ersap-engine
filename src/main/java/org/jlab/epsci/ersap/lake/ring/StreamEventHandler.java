package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.EventHandler;
import org.jlab.epsci.ersap.util.EUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class StreamEventHandler implements EventHandler<StreamEvent> {

    private int prevId;
    private long prevRecord;

    @Override
    public void onEvent(StreamEvent streamEvent, long sequence, boolean endOfBatch) throws Exception {

/*
        int currId = streamEvent.getSatreamId();

        if (currId != prevId) {
            long currRecord = streamEvent.getRecordNumber();

            if (prevRecord < currRecord) {
                decodePayload(streamEvent.getPayload());
                prevId = currId;
                prevRecord = currRecord;
            } else if (prevRecord == currRecord) {
                decodePayload(streamEvent.getPayload());

                //decodeAndSend(streamEvent.getPayload());
                // publish it to the new ring

                prevRecord = 0;
                prevId = 0;
            }
            prevId = currId;
        }
*/

        // For testing purposes
        decodePayload(streamEvent.getPayload());
//        decodeAndSend(streamEvent.getPayload());


    }


    private void decodePayload(byte[] b) {
        if (b != null) {
            ByteBuffer bb = ByteBuffer.wrap(b);
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
                        bb.position(slot_ind[i]);
//                    System.out.println("at entrance "+bb.position()+
//                            " words = "+slot_len[i]/4+
//                            " slot_ind = "+slot_ind[i]+
//                            " slot_len = "+slot_len[i]);
                        for (int j = 0; j < slot_len[i]; j++) {
                            int val = bb.getInt();
                            int type = 0;
                            int rocid = 0;
                            int slot = 0;
                            int q = 0;
                            int ch = 0;
                            int t = 0;
                            if ((val & 0x80000000) == 0x80000000) {
                                type = (val >> 15) & 0xFFFF;
                                rocid = (val >> 8) & 0x007F;
                                slot = (val) & 0x001F;
                            }
                            if (type == 0x0001) /* FADC hit type */ {
                                q = (val) & 0x1FFF;
                                ch = (val >> 13) & 0x000F;
                                t = ((val >> 17) & 0x3FFF) * 4;
                            }
                            System.out.println("rocid = " + rocid
                                    + " slot = " + slot
                                    + " ch = " + ch
                                    + " q = " + q
                                    + " t = " + t
                            );
                        }
                    }
                }
            }
        }
    }

    private void decodeAndSend(byte[] b) {
        if (b != null) {
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            int[] slot_ind = new int[8];
            int[] slot_len = new int[8];
            long tag = EUtil.getUnsignedInt(bb);

            if ((tag & 0x8FFF8000L) == 0x80000000L) {

                HitTime hitTime = new HitTime();

                for (int jj = 0; jj < 8; jj++) {
                    slot_ind[jj] = EUtil.getUnsignedShort(bb);
                    slot_len[jj] = EUtil.getUnsignedShort(bb);
                }
                for (int i = 0; i < 8; i++) {
                    if (slot_len[i] > 0) {
                        bb.position(slot_ind[i]);
                        for (int j = 0; j < slot_len[i]; j++) {
                            int val = bb.getInt();
                            int type = 0;
                            RawData rawData = new RawData();

                            if ((val & 0x80000000) == 0x80000000) {
                                type = (val >> 15) & 0xFFFF;
                                rawData.setRocId((val >> 8) & 0x007F);
                                rawData.setSlot((val) & 0x001F);
                            }
                            if (type == 0x0001) /* FADC hit type */ {
                                rawData.setQ((val) & 0x1FFF);
                                rawData.setChannel((val >> 13) & 0x000F);
                                int t = ((val >> 17) & 0x3FFF) * 4;
                                hitTime.addHitTime(t, rawData);
                            }
                            System.out.println(rawData);
                        }
                    }
                }
            }
        }
    }
}



