package org.jlab.epsci.ersap.lake.ring;


public class RawData {

        private int rocId;
        private int slot;
        private int channel;
        private int q;


        public int getRocId() {
            return rocId;
        }

        public void setRocId(int rocId) {
            this.rocId = rocId;
        }

        public int getSlot() {
            return slot;
        }

        public void setSlot(int slop) {
            this.slot = slop;
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

        public void reset(){
            rocId = 0;
            slot = 0;
            channel = 0;
            q = 0;
        }

    @Override
    public String toString() {
        return "RawData{" +
                "rocId=" + rocId +
                ", slot=" + slot +
                ", channel=" + channel +
                ", q=" + q +
                '}';
    }
}
