package org.jlab.epsci.ersap.lake.ring;

import org.jlab.epsci.ersap.util.EUtil;
import org.jlab.epsci.ersap.util.SlidingWindowStream;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HitFinder {

    private BigInteger frameStartTime_ns = BigInteger.ZERO;
    private int frameLength_ns = 64000;
    private int slice_ns = 32;
    private int slidingWindowSize = 3;

    private static Map<Integer, List<Integer>> slideCharges = new HashMap<>();
    private static Map<Integer, List<ChargeTime>> hits = new HashMap<>();
    private List<AdcHit> vtpStream;

    HitFinder reset() {
        slideCharges.clear();
        hits.clear();
        if (vtpStream != null && !vtpStream.isEmpty()) {
            vtpStream.clear();
        }
        return this;
    }

    public HitFinder stream(List<AdcHit> stream) {
        this.vtpStream = stream;
        return this;
    }

    HitFinder frameStartTime(BigInteger frameStartTime) {
        this.frameStartTime_ns = frameStartTime;
        return this;
    }

    HitFinder frameLength(int frameLength) {
        this.frameLength_ns = frameLength;
        return this;
    }

    HitFinder sliceSize(int slice) {
        this.slice_ns = slice;
        return this;
    }

    HitFinder windowSize(int slide) {
        this.slidingWindowSize = slide;
        return this;
    }

    public BigInteger getFrameStartTime_ns() {
        return frameStartTime_ns;
    }

    Map<Integer, List<ChargeTime>> slide() {
        slide(getSlices(vtpStream), slidingWindowSize);
        findHits();
        return hits;
    }

    private Map<Integer, Integer> streamSlice(BigInteger leading, BigInteger trailing,
                                              List<AdcHit> vtpStream) {
        return vtpStream
                .stream()
                .filter(t -> (t.getTime().compareTo(leading) > 0) && (t.getTime().compareTo(trailing) < 0))
                .collect(Collectors.groupingBy(
                        v -> EUtil.encodeCSC(v.getCrate(), v.getSlot(), v.getChannel()),
                        Collectors.summingInt(AdcHit::getQ)));
    }

    private List<Map<Integer, Integer>> getSlices(List<AdcHit> frame) {
        List<Map<Integer, Integer>> streamSlices = new ArrayList<>();
        for (long i = 0; i < frameLength_ns; i += slice_ns) {
            streamSlices.add(
                    streamSlice(
                            frameStartTime_ns.add(EUtil.toUnsignedBigInteger(i)),
                            frameStartTime_ns.add(EUtil.toUnsignedBigInteger(slice_ns)),
                            frame)
            );
        }
        return streamSlices;
    }

    private void slide(List<Map<Integer, Integer>> slices, int windowSize) {
        SlidingWindowStream
                .slidingStream(slices, windowSize)
                .map(s -> s.collect(Collectors.toList()))
                .forEach(HitFinder::slideWindowSum);
    }

    private static void slideWindowSum(List<Map<Integer, Integer>> slideWindow) {
        Map<Integer, Integer> sws = new HashMap<>();
        for (Map<Integer, Integer> m : slideWindow) {
            for (int k : m.keySet()) {
                if (sws.containsKey(k)) {
                    int c = sws.get(k) + m.get(k);
                    sws.put(k, c);
                } else {
                    sws.put(k, m.get(k));
                }
            }
        }

        for (int k : sws.keySet()) {
            if (slideCharges.containsKey(k)) {
                slideCharges.get(k).add(sws.get(k));
            } else {
                List<Integer> l = new ArrayList<>();
                l.add(sws.get(k));
                slideCharges.put(k, l);
            }
        }
    }

    private void findHits() {
        for (Integer csc : slideCharges.keySet()) {
            List<Integer> l = slideCharges.get(csc);
            for (int i = 1; i < l.size() - 1; i++) {
                if ((l.get(i - 1) < l.get(i)) && (l.get(i + 1) < l.get(i))) {
                    BigInteger time = frameStartTime_ns.add(EUtil.toUnsignedBigInteger(i * slice_ns));
                    if (hits.containsKey(csc)) {
                        hits.get(csc).add(new ChargeTime(time, l.get(i)));
                    } else {
                        List<ChargeTime> m = new ArrayList<>();
                        m.add(new ChargeTime(time, l.get(i)));
                        hits.put(csc, m);
                    }
                }
            }
        }
    }
}
