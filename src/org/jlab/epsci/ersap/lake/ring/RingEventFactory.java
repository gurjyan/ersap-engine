package org.jlab.epsci.ersap.lake.ring;

import com.lmax.disruptor.EventFactory;

public class RingEventFactory implements EventFactory<RingEvent> {

    @Override
    public RingEvent newInstance() {
        return new RingEvent();
    }
}
