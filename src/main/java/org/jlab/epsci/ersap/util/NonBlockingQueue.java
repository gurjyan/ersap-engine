package org.jlab.epsci.ersap.util;

import java.util.concurrent.ArrayBlockingQueue;

public class NonBlockingQueue<E> extends ArrayBlockingQueue<E> {
    private static final long serialVersionUID = -7772085623838075506L;

    // Size of the queue
    private final int size;

    // Constructor
    public NonBlockingQueue(int size) {
        // Creates an ArrayBlockingQueue with the given (fixed) capacity and default access policy
        super(size);
        this.size = size;
    }

    @Override
    public boolean add(E e) {
        // Check if queue full already?
        if (super.size() == this.size) {
            // remove element from queue if queue is full
            this.remove();
        }
        return super.add(e);
    }
}
