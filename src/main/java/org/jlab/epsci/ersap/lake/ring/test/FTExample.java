package org.jlab.epsci.ersap.lake.ring.test;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FTExample {

    /**
     * arg1 = port1
     * arg2 = port2
     * arg3 = fileName to store arrayList of AdcHit objects
     * @param args argument list
     */
    public static void main(String[] args) {
        int vtpPort1 = Integer.parseInt(args[0]);
        int vtpPort2 = Integer.parseInt(args[1]);

        // Thread per VTP stream
        ExecutorService pool = Executors.newFixedThreadPool(2);
        // Single stream handler for both streamReceivers: crate level aggregation
        RingBufferHandler ringBufferHandler1 = new RingBufferHandler(args[2]);
//        RingBufferHandler ringBufferHandler2 = new RingBufferHandler(args[3]);
        // Create streamReceivers
        StreamReceiver s1 = new StreamReceiver(vtpPort1, 1, 256, ringBufferHandler1, 10);
        StreamReceiver s2 = new StreamReceiver(vtpPort2, 2, 256, ringBufferHandler1, 10);
        // start the threads
        pool.execute(s1);
        pool.execute(s2);
    }

}
