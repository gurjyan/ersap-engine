package org.jlab.epsci.ersap.lake.ring;

import java.math.BigInteger;
import java.util.Objects;

public class ChargeTime {
private BigInteger time;
private int charge;

ChargeTime(BigInteger time, int charge){
    this.time = time;
    this.charge = charge;
}
    public BigInteger getTime() {
        return time;
    }

    public int getCharge() {
        return charge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChargeTime that = (ChargeTime) o;
        return getCharge() == that.getCharge() &&
                getTime().equals(that.getTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTime(), getCharge());
    }

    @Override
    public String toString() {
        return "ChargeTime{" +
                "time=" + time +
                ", charge=" + charge +
                '}';
    }
}
