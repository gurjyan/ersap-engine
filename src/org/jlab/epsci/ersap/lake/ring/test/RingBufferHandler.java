package org.jlab.epsci.ersap.lake.ring.test;

import com.lmax.disruptor.EventHandler;
import org.jlab.epsci.ersap.lake.ring.AdcHit;
import org.jlab.epsci.ersap.lake.ring.RingEvent;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Consumer thread on the Disruptor ring buffer.
 * Called after RingEvent is stored in the ring.
 *
 */
public class RingBufferHandler implements EventHandler<RingEvent> {

    private ExecutorService pool = Executors.newFixedThreadPool(4);

    private ObjectOutputStream oos;

    private BigInteger frame_time_ns;
    private ArrayList<AdcHit> hits = new ArrayList<>();

    RingBufferHandler(String fileName) {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(fileName, true);
            oos = new ObjectOutputStream(fos);
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onEvent(RingEvent streamEvent, long sequence, boolean endOfBatch) throws Exception {
        BigInteger currRecord = streamEvent.getRecordNumber();

        System.out.println("DDD => streamId = "+streamEvent.getStreamId()
                + " record = " + streamEvent.getRecordNumber()
                + " sequence = " + sequence
                + " endOfBatch = " + endOfBatch
        );
        frame_time_ns = currRecord.multiply(EUtil.toUnsignedBigInteger(65536L));

        // Record from the first stream number. Extracts the payload.
        byte[] d1 = streamEvent.getPayload();
        // Start a thread to parse the payloads from both streams and aggregate them
//        if (d1 != null) {
//            pool.execute(new StreamPayloadParser(d1, currRecord, oos));
//        }

        decodePayload_x(d1);
//        try {
//            oos.writeObject(hits);
//            if(endOfBatch) oos.flush();
////            oos.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
        // For testing purposes
        // EUtil.decodeVtpPayload(streamEvent.getPayload());
        // decodeAndSend(streamEvent.getPayload());
    }

    public static void decodePayload(byte[] payload) {
        if (payload == null) return;
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
                    bb.position(slot_ind[i]*4);
                    int type = 0;
                    for (int j = 0; j < slot_len[i]; j++) {
                        int val = bb.getInt();
                        if ((val & 0x80000000) == 0x80000000) {
                            type = (val >> 15) & 0xFFFF;
                            int rocid = (val >> 8) & 0x007F;
                            int slot = (val) & 0x001F;
                        } else if (type == 0x0001) /* FADC hit type */ {
                            int q = (val) & 0x1FFF;
                            int ch = (val >> 13) & 0x000F;
                            int t = ((val >> 17) & 0x3FFF) * 4;
                        }
                    }
                }
            }
        } else {
            System.out.println("........");
            System.exit(0);
        }
    }
    private void decodePayload_x(byte[] payload) {
        if (payload == null) return;
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
                            hits.add(hit);
                        }
                    }
                }
            }
        } else {
            System.out.println("parser error: wrong tag");
            System.exit(0);
        }
    }
}



