package com.synacor.soa.ark;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;

public class WildChildTest {
	private String connectString = "localhost:2181";
	private Timing timing = new Timing();
	private Logger log = Logger.getLogger(WildChildTest.class);
//	private TestingServer testingServer;
	
    private static class CountCuratorWatcher implements CuratorWatcher
    {
    	private final Set<String> created = new HashSet<String>();
    	private final Set<String> changed = new HashSet<String>();
    	private final Set<String> deleted = new HashSet<String>();
    	private Logger log = Logger.getLogger(CountCuratorWatcher.class);
    	
        public void process(WatchedEvent event) throws Exception
        {
        	log.info("LEAF EVENT: " + event.getPath() + " " + event.getType().name());
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
        CuratorFramework serverFramework = newClient();
        new WildChild(serverFramework, "/services/.*/deployments/.*/instances/.*/lifecycleState", watcher);
		log.info("Initial node count: " + watcher.created.size());

		CuratorFramework client = newClient();
		deleteRecursive(client, "/services");
		log.info("Deleted: " + watcher.deleted.size());

		client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.0/instances/1/lifecycleState");
		client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.0/instances/2/lifecycleState");
		client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.1/instances/1/lifecycleState");
		client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.1/instances/2/lifecycleState");
		client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/instances/1/lifecycleState");
		client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/instances/2/lifecycleState");

		client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/instances/2/DOES_NOT_MATCH");
		client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/DOES_NOT_MATCH/2/lifecycleState");

		Assert.assertTrue(watcher.created.size() == 6);
		Assert.assertTrue(watcher.deleted.size() == 6);
		Assert.assertTrue(watcher.changed.size() == 0);
	}

	@Before
	public void init() throws Exception {
//		testingServer = new TestingServer();
//		connectString = testingServer.getConnectString();
	}
	
	@After
	public void teardown() throws Exception {
//		testingServer.close();
	}
	
    private void deleteRecursive(CuratorFramework client, String path) throws Exception {
    	if(client.checkExists().forPath(path) == null) return;
    	List<String> children = client.getChildren().forPath(path);
    	for(String child : children) {
    		deleteRecursive(client, path + "/" + child);
    	}
    	client.delete().forPath(path);
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
