package com.hazelcast.jpoint.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;

/**
 * Created by vikgamov on 4/24/16.
 */
public class MemberRunnable
    implements Runnable, HazelcastInstanceAware {

    transient HazelcastInstance hazelcastInstance;

    @Override public void run() {
        final Member localMember = hazelcastInstance.getCluster().getLocalMember();
        System.out.println(localMember.getAddress());

    }

    @Override public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
