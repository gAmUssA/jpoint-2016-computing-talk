package com.hazelcast.jpoint.mr;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jpoint.mr.support.ToStringPrettyfier;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordCountExample {

    private static final String[] DATA_RESOURCES_TO_LOAD = {"eugene.txt"};

    private static final String MAP_NAME = "pushkin";
    private static final String TRACKER_NAME = "pushkin";

    public static void main(String[] args)
        throws Exception {

        // Prepare Hazelcast cluster
        HazelcastInstance hazelcastInstance = buildCluster(3);

        try {

            // Read data
            fillMapWithData(hazelcastInstance);

            JobTracker tracker = hazelcastInstance.getJobTracker(TRACKER_NAME);

            IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
            KeyValueSource<String, String> source = KeyValueSource.fromMap(map);

            Job<String, String> job = tracker.newJob(source);

            final JobCompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new TokenizerMapper())
                // Activate Combiner to add combining phase!
                // .combiner(new WordcountCombinerFactory())
                .reducer(new WordcountReducerFactory())
                //                .submit();
                // add collator for sorting and top10
                .submit(new WordcountCollator());

            future.andThen(new ExecutionCallback<List<Map.Entry<String, Integer>>>() {
                @Override public void onResponse(List<Map.Entry<String, Integer>> response) {
                    System.out.println(ToStringPrettyfier.toString(response));
                }

                @Override public void onFailure(Throwable t) {

                }
            });

            //System.out.println(ToStringPrettyfier.toString(future.get()));

        } finally {
            // Shutdown cluster
            //Hazelcast.shutdownAll();
        }
    }

    private static HazelcastInstance buildCluster(int memberCount) {
        Config config = new XmlConfigBuilder().build();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getJoin().getTcpIpConfig().setMembers(Arrays.asList(new String[] {"127.0.0.1"}));

        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[memberCount];
        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = Hazelcast.newHazelcastInstance(config);
        }
        return hazelcastInstances[0];
    }

    private static void fillMapWithData(HazelcastInstance hazelcastInstance)
        throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = WordCountExample.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            map.put(file, sb.toString());

            is.close();
            reader.close();
        }
    }

    public static String cleanWord(String word) {
        return word.replaceAll("[^A-Za-zA-Яа-я]", "");
    }
}
