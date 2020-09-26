package org.jlab.epsci.ersap.util;

import org.jlab.epsci.ersap.lake.ring.AdcHit;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.summingInt;

public class EUtil {
    private static final byte[] i32 = new byte[4];
    private static final byte[] i64 = new byte[8];

    public static short getUnsignedByte(ByteBuffer bb) {
        return ((short) (bb.get() & 0xff));
    }

    public static void putUnsignedByte(ByteBuffer bb, int value) {
        bb.put((byte) (value & 0xff));
    }

    public static short getUnsignedByte(ByteBuffer bb, int position) {
        return ((short) (bb.get(position) & (short) 0xff));
    }

    public static void putUnsignedByte(ByteBuffer bb, int position, int value) {
        bb.put(position, (byte) (value & 0xff));
    }


    public static int getUnsignedShort(ByteBuffer bb) {
        return (bb.getShort() & 0xffff);
    }

    public static void putUnsignedShort(ByteBuffer bb, int value) {
        bb.putShort((short) (value & 0xffff));
    }

    public static int getUnsignedShort(ByteBuffer bb, int position) {
        return (bb.getShort(position) & 0xffff);
    }

    public static void putUnsignedShort(ByteBuffer bb, int position, int value) {
        bb.putShort(position, (short) (value & 0xffff));
    }


    public static long getUnsignedInt(ByteBuffer bb) {
        return ((long) bb.getInt() & 0xffffffffL);
    }

    public static void putUnsignedInt(ByteBuffer bb, long value) {
        bb.putInt((int) (value & 0xffffffffL));
    }

    public static long getUnsignedInt(ByteBuffer bb, int position) {
        return ((long) bb.getInt(position) & 0xffffffffL);
    }

    public static void putUnsignedInt(ByteBuffer bb, int position, long value) {
        bb.putInt(position, (int) (value & 0xffffffffL));
    }

    public static long readLteUnsined32(DataInputStream dataInputStream) {
        ByteBuffer bb = null;
//        byte[] i32 = new byte[4];
        try {
            dataInputStream.readFully(i32);
            bb = ByteBuffer.wrap(i32);
            bb.order(ByteOrder.LITTLE_ENDIAN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert bb != null;
        return getUnsignedInt(bb);
    }

    public static int readUnsined32(DataInputStream dataInputStream) throws IOException {
        int ch1 = dataInputStream.read();
        int ch2 = dataInputStream.read();
        int ch3 = dataInputStream.read();
        int ch4 = dataInputStream.read();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }


    public static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L)
            return BigInteger.valueOf(i);
        else {
            int upper = (int) (i >>> 32);
            int lower = (int) i;

            return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).
                    add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
        }
    }

    public static BigInteger readLteUnsigned64(DataInputStream dataInputStream) {
        ByteBuffer bb = null;
//        byte[] i64 = new byte[8];
        try {
            dataInputStream.readFully(i64);
            bb = ByteBuffer.wrap(i64);
            bb.order(ByteOrder.LITTLE_ENDIAN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert bb != null;
        return toUnsignedBigInteger(bb.getLong());
    }

    public static BigInteger readLteUnsignedSwap64(DataInputStream dataInputStream) {
        ByteBuffer bb = null;
//        byte[] i64 = new byte[8];
        try {
            dataInputStream.readFully(i64);
            bb = ByteBuffer.wrap(i64);
            bb.order(ByteOrder.LITTLE_ENDIAN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert bb != null;
        return toUnsignedBigInteger(llSwap(bb.getLong()));
    }

    public static long[] readLtPayload(DataInputStream dataInputStream, long payload_length) {
        long[] payload = new long[(int) payload_length / 4];
        int j = 0;
        for (long i = 0; i < payload_length; i = i + 4) {
            payload[j] = readLteUnsined32(dataInputStream);
            j = j + 1;
        }
        return payload;
    }

    public static long llSwap(long l) {
        long x = l >> 32;
        x = x | l << 32;
        return x;
    }

    public static byte[] long2ByteArray(long lng) {
        byte[] b = new byte[]{
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
        return b;
    }

    public static void busyWaitMicros(long delay) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < delay) ;
    }

    public static <T> T requireNonNull(T obj, String desc) {
        return Objects.requireNonNull(obj, "null " + desc);
    }


    public static void decodeVtpPayload(byte[] payload) {
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
//                    System.out.println("at entrance "+bb.position()+
//                            " words = "+slot_len[i]/4+
//                            " slot_ind = "+slot_ind[i]+
//                            " slot_len = "+slot_len[i]);
                    int type = 0;
                    for (int j = 0; j < slot_len[i]; j++) {
                        int val = bb.getInt();
                        if ((val & 0x80000000) == 0x80000000) {
                            type = (val >> 15) & 0xFFFF;
                            int rocid = (val >> 8) & 0x007F;
                            int slot = (val) & 0x001F;
                            System.out.println("\nrocid = " + rocid + " slot = " + slot);
                            System.out.println("----------------------------------");
                        } else if (type == 0x0001) /* FADC hit type */ {
                            int q = (val) & 0x1FFF;
                            int ch = (val >> 13) & 0x000F;
                            int t = ((val >> 17) & 0x3FFF) * 4;
                            System.out.println("q = " + q + " ch = " + ch + " t = " + t);
                        }
                    }
                }
            }
        } else {
            System.out.println("........");
            System.exit(0);
        }
    }

    public static byte[] addByteArrays(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    public static byte[] object2ByteArray(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();
        return bos.toByteArray();
    }

    public static Object byteArray2Object(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public static Map<Integer, Integer> streamSlice(BigInteger leading, BigInteger trailing,
                                                    ArrayList<AdcHit> vtpStream) {
        return vtpStream
                .stream()
                .filter(t -> (t.getTime().compareTo(leading) > 0) && (t.getTime().compareTo(trailing) < 0))
                .collect(Collectors.groupingBy(
                        v -> encodeCSC(v.getCrate(), v.getSlot(), v.getChannel()), summingInt(AdcHit::getQ)));
    }


    public static int encodeCSC(int crate, int slot, int channel) {
        return (crate << 16) | (slot << 8) | (channel << 4);
    }

    public static int decodeCrateNumber(int csc) {
        return csc >>> 16;
    }

    public static int decodeSlotNumber(int csc) {
        return csc & 0x000000f0;
    }

    public static int decodeChannelNumber(int csc) {
        return csc & 0x0000000f;
    }

    public static Map<String, List<Integer>> getMultiplePeaks(int[] arr) {
        List<Integer> pos = new ArrayList<>();
        List<Integer> pea = new ArrayList<>();
        Map<String, List<Integer>> ma = new HashMap<>();
        int cur = 0, pre = 0;
        for (int a = 1; a < arr.length; a++) {
            if (arr[a] > arr[cur]) {
                pre = cur;
                cur = a;
            } else {
                if (arr[a] < arr[cur])
                    if (arr[pre] < arr[cur]) {
                        pos.add(cur);
                        pea.add(arr[cur]);
                    }
                pre = cur;
                cur = a;
            }

        }
        ma.put("pos", pos);
        ma.put("peaks", pea);
        return ma;
    }
}
