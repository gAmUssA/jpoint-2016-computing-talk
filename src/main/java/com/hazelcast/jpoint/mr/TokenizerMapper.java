package com.hazelcast.jpoint.mr;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.StringTokenizer;

public class TokenizerMapper
    implements Mapper<String, String, String, Integer> {

    private static final Integer ONE = Integer.valueOf(1);

    @Override
    public void map(String key, String value, Context<String, Integer> context) {
        StringTokenizer tokenizer = new StringTokenizer(value);
        while (tokenizer.hasMoreTokens()) {
            String word = WordCountExample.cleanWord(tokenizer.nextToken());
            if (word.length() >= 4) {
                context.emit(word.toLowerCase(), ONE);
            }
        }
    }
}
