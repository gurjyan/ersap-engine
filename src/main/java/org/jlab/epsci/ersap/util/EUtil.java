package org.jlab.epsci.ersap.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

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

    public static void busyWaitMicros(long delay){
        long start = System.nanoTime();
        while(System.nanoTime() - start < delay);
    }

    public static <T> T requireNonNull(T obj, String desc) {
        return Objects.requireNonNull(obj, "null " + desc);
    }
}
