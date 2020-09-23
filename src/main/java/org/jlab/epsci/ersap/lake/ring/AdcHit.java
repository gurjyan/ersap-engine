package org.jlab.epsci.ersap.lake.ring;


import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

public class AdcHit implements Serializable {

        private int crate;
        private int slot;
        private int channel;
        private int q;
        private BigInteger time;


    public int getCrate() {
        return crate;
    }

    public void setCrate(int crate) {
        this.crate = crate;
    }

    public int getSlot() {
        return slot;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public int getQ() {
        return q;
    }

    public void setQ(int q) {
        this.q = q;
    }

    public BigInteger getTime() {
        return time;
    }

    public void setTime(BigInteger time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "AdcHit{" +
                "crate=" + crate +
                ", slot=" + slot +
                ", channel=" + channel +
                ", q=" + q +
                ", time=" + time +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdcHit adcHit = (AdcHit) o;
        return getCrate() == adcHit.getCrate() &&
                getSlot() == adcHit.getSlot() &&
                getChannel() == adcHit.getChannel() &&
                getQ() == adcHit.getQ() &&
                getTime().equals(adcHit.getTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCrate(), getSlot(), getChannel(), getQ(), getTime());
    }
}
