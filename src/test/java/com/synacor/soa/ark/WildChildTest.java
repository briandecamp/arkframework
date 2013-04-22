package com.synacor.soa.ark;

import java.util.List;

import org.junit.Test;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.Timing;

public class WildChildTest {
	private Timing timing = new Timing();
	String connectString = "localhost:2181";
	
	public static void main(String[] argv) throws Exception {
		new WildChildTest().testCuratorWatcher();
	}
	
	@Test
    public void testCuratorWatcher() throws Exception
    {
        CuratorFramework client = newClient();
        try
        {
			deleteRecursive(client, "/services");
			timing.sleepABit();
			client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.0/instances/1/lifecycleState");
			client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.0/instances/2/lifecycleState");
			client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.1/instances/1/lifecycleState");
			client.create().creatingParentsIfNeeded().forPath("/services/a/deployments/1.0.1/instances/2/lifecycleState");
			client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/instances/1/lifecycleState");
			client.create().creatingParentsIfNeeded().forPath("/services/b/deployments/1.0.0/instances/2/lifecycleState");

            timing.sleepABit();
        }
        finally
        {
            Closeables.closeQuietly(client);
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

    private void deleteRecursive(CuratorFramework client, String path) throws Exception {
    	if(client.checkExists().forPath(path) == null) return;
    	List<String> children = client.getChildren().forPath(path);
    	for(String child : children) {
    		deleteRecursive(client, path + "/" + child);
    	}
    	client.delete().forPath(path);
    }
}
