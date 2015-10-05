package com.hazelcast.jpoint.mr;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by vikgamov on 4/24/16.
 */
public class WordcountCollator
    implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Integer>>> {
    @Override public List<Map.Entry<String, Integer>> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Map.Entry<String, Integer>> result = new ArrayList<>();
        for (Map.Entry<String, Integer> value : values) {
            result.add(value);
        }
        Collections.sort(result, (o1, o2) -> Integer.compare(o2.getValue(), o1.getValue()));

        return result.subList(0, 10);
    }
}
