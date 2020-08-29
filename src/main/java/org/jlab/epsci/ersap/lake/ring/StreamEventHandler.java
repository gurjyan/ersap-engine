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
//        decodePayload(streamEvent.getPayload());
//        decodeAndSend(streamEvent.getPayload());

    }



}



