package org.jlab.epsci.ersap.lake.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jlab.epsci.ersap.util.NonBlockingQueue;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

public class CQTest {
    private static double totalData;
    private static int loop = 10;
    private static int rate;

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new PrintRates(), 0, 1000);

        NonBlockingQueue<byte[]> localQueue = new NonBlockingQueue<>(1000);

        int d_size = Integer.parseInt(args[0]);

        String path = "/mnt/optane1/test/gurjyan/lqueue/";
//        String path = "queue";

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .storeFileListener(new StoreFileListener() {
                    @Override
                    public void onReleased(int i, File file) {
//                System.out.println("File is not in use and is ready for deletion: " + file.getName());
                        file.delete();
//                System.out.println("File deleted:  " + file.getName() );
                    }

                    @Override
                    public void onAcquired(int cycle, File file) {
//                System.out.println("File is use for reading: " + file.getName());
                    }
                })
                .checkInterrupts(false)
                .doubleBuffer(false)
                .ringBufferForceCreateReader(true)
                .strongAppenders(false)
                .epoch(0)
                .enableRingBufferMonitoring(false)
                .build();

//        ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
//                .rollCycle(RollCycles.TEST_SECONDLY)
//                .build();
//        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
        ExcerptAppender appender = queue.acquireAppender();
        ExcerptTailer trailer = queue.createTailer("a");
        byte[] d = new byte[d_size];
        Bytes data = Bytes.allocateElasticDirect();
//        for (int i= 0; i<d_size;i++){
//            d[i] = 1;
//        }
        data.write(d);
        Bytes bytes2 = Bytes.allocateElasticDirect();

        while (true) {
            appender.writeBytes(data);
            bytes2.clear();
            trailer.readBytes(bytes2);
//            localQueue.add(d);
            rate++;
            totalData += d_size;
//            EUtil.busyWaitMicros(100000);
        }
    }

    private static class PrintRates extends TimerTask {

        @Override
        public void run() {
            if (loop <= 0) {
                System.out.println("event rate =" + rate / 1000
                        + " KHz.  data rate =" + totalData / 1000 + " KB/s.");
                loop = 10;
            }
            rate = 0;
            totalData = 0;
            loop--;
        }
    }

}
