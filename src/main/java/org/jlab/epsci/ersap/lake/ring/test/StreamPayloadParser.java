package org.jlab.epsci.ersap.lake.ring.test;

import org.jlab.epsci.ersap.lake.ring.AdcHit;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;


/**
 * Parses stream payload, creates AdcHit objects
 * and writes it into an ArrayList.
 */
public class StreamPayloadParser implements Runnable {

    private ArrayList<AdcHit> hits = new ArrayList<>();

    private BigInteger frame_time_ns;
    private byte[] d1;

    private ObjectOutputStream oos;

    StreamPayloadParser(byte[] d1, BigInteger recordNumber, ObjectOutputStream oos) {
        frame_time_ns = recordNumber.multiply(EUtil.toUnsignedBigInteger(65536L));
        this.d1 = d1;
        this.oos = oos;
    }

    private void decodePayload(byte[] payload) {
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

    @Override
    public void run() {
        decodePayload(d1);
        try {
            oos.writeObject(hits);
//            oos.flush();
//            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
