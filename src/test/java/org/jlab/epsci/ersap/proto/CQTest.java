package org.jlab.epsci.ersap.proto;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import org.jlab.epsci.ersap.util.EUtil;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class CQTest {
    private static double totalData;
    private static int loop = 10;
    private static int rate;
    private static File f;

    private static boolean deleteFile = false;

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);

        if (args.length >=2) deleteFile = true;

        int d_size = Integer.parseInt(args[0]);

        String path = "/mnt/ramdisk";

        //        String path = "qtest";
        f = new File (path+"/20200722.cq4");
        ChronicleQueue queue = ChronicleQueue.singleBuilder("./build/roll")
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();
//        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
        ExcerptAppender appender = queue.acquireAppender();
        ExcerptTailer tailer = queue.createTailer();
        byte[] d = new byte[d_size];
        Bytes data = Bytes.allocateElasticDirect();
        data.write(d);
        Bytes bytes2 = Bytes.allocateElasticDirect();

        while (true) {
            appender.writeBytes(data);
//            bytes2.clear();
//            tailer.readBytes(bytes2);
            rate++;
            totalData += d_size;
            EUtil.busyWaitMicros(50);
        }
    }

    private static class PrintRates extends TimerTask {

        @Override
        public void run() {
            if (loop <= 0) {
                System.out.println("event rate =" + rate/1000
                        + " KHz.  data rate =" + totalData/1000 + " KB/s.");
                loop = 10;
                if(deleteFile) {
                    try {
                        f.delete();
                        f.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            // delete the file
            rate = 0;
            totalData = 0;
            loop--;
        }
    }

}
