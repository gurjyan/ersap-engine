package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.EventFactory;

public class StreamEventFactory implements EventFactory<StreamEvent> {

    @Override
    public StreamEvent newInstance() {
        return new StreamEvent();
    }
}
