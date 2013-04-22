package com.synacor.soa.ark;

import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.Timing;

public class WildChildServerTest {
	private String connectString = "localhost:2181";
	private Timing timing = new Timing();
	
	public static void main(String[] argv) throws Exception {
		new WildChildServerTest().testServer();
	}
	
    private static class CountCuratorWatcher implements CuratorWatcher
    {
    	private final Set<String> created = new HashSet<String>();
    	private final Set<String> changed = new HashSet<String>();
    	private final Set<String> deleted = new HashSet<String>();
    	
        public void process(WatchedEvent event) throws Exception
        {
        	System.out.println("LEAF EVENT: " + event.getPath() + " " + event.getType().name());
        	if(event.getType() == EventType.NodeCreated) {
        		created.add(event.getPath());
        	} else if(event.getType() == EventType.NodeDeleted) {
        		deleted.add(event.getPath());
        	} else if(event.getType() == EventType.NodeDataChanged) {
        		changed.add(event.getPath());
        	}
        }
    }

	@Test
	public void testServer() throws Exception {
        final CountCuratorWatcher watcher = new CountCuratorWatcher();
        Set<String> currentNodes = new WildChild(newClient(), "/services/.*/deployments/.*/instances/.*/lifecycleState", watcher).getMatchingLeaves();
		System.out.println("Initial node count: " + currentNodes.size());
		synchronized(this) {
			wait();
		}
	}

    private CuratorFramework newClient() {
		CuratorFramework client = CuratorFrameworkFactory
				.builder()
				.sessionTimeoutMs(timing.session())
				.connectionTimeoutMs(timing.connection())
				.namespace("unit-test")
				.connectString(connectString)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		return client;
	}
}
