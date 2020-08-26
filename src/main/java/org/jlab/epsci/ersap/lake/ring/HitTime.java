package org.jlab.epsci.ersap.lake.ring;

import java.util.HashMap;

public class HitTime {

    private HashMap<Integer, RawData> map = new HashMap<>();

    public void addHitTime(int time, RawData data){
        map.put(time,data);
    }

    public HashMap getHitTimeMap(){
        return map;
    }

    public void clear(){
        map.clear();
    }
}
