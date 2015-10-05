
package com.hazelcast.jpoint.mr;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class WordcountReducerFactory
        implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String key) {
        return new WordcountReducer();
    }

    private static class WordcountReducer
            extends Reducer<Integer, Integer> {

        private volatile int count;

        @Override
        public void reduce(Integer value) {
            // Use with and without Combiner to show combining phase!
            // System.out.println("Retrieved value: " + value);
            count += value;
        }

        @Override
        public Integer finalizeReduce() {
            return count == 0 ? null : count;
        }
    }
}
