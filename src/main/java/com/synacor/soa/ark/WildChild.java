package com.synacor.soa.ark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;

public class WildChild {
	CuratorFramework client;
	private String path;
	private String wildPath;
	private String matchCriteria; // the next part of the fullPath

	private CuratorWatcher leafWatcher;
	private Map<String, WildChild> trackedChildren = new HashMap<String, WildChild>();

	public WildChild(CuratorFramework client, String wildPath, CuratorWatcher leafWatcher) throws Exception {
		this(client, "", wildPath, leafWatcher);
	}
	
	private WildChild(CuratorFramework client, String path, String wildPath, CuratorWatcher leafWatcher) throws Exception {
		this.client = client;
		this.path = path;
		this.wildPath = wildPath;
		this.leafWatcher = leafWatcher;

		int index = path.split("/").length;
		this.matchCriteria = wildPath.split("/")[index];

		List<String> children = client.getChildren().usingWatcher(new WildChildWatcher()).forPath(path);
		for(String child : children) {
			String childPath = path + "/" + child;
			boolean childIsLeaf = childPath.split("/").length == wildPath.split("/").length;
			trackedChildren.put(child, childIsLeaf ? null : new WildChild(client, childPath, wildPath, leafWatcher));
			if(childIsLeaf) {
				WatchedEvent createdEvent = new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, childPath);
				leafWatcher.process(createdEvent);
			}
		}
	}

	public Set<String> getMatchingLeaves() {
		boolean isChildLeaf = path.split("/").length+1 == wildPath.split("/").length;
		Set<String> collect = new HashSet<String>();
		for(String child : trackedChildren.keySet()) {
			if(isChildLeaf) {
				collect.add(path + "/" + child);
			} else {
				collect.addAll(trackedChildren.get(child).getMatchingLeaves());
			}
		}
		return collect;
	}

	/**
	 * Watcher class that monitors a node for child changes,
	 * and manages setting watchers on child nodes or notifying the primary watcher.
	 */
	private class WildChildWatcher implements CuratorWatcher {
		private List<String> getChildrenSafe() {
			try {
				return client.getChildren().usingWatcher(this).forPath(path);
			} catch (KeeperException.NoNodeException exc) {
				return new ArrayList<String>();
			} catch (Exception exc) {
				throw new RuntimeException(exc);
			}
		}
		
		public void process(WatchedEvent event) throws Exception {
			if(!path.equals(path)) throw new RuntimeException("incorrect path");
			
			boolean childIsLeaf = wildPath.split("/").length == path.split("/").length+1;

			if(event.getType() == EventType.NodeChildrenChanged) {
				List<String> children = getChildrenSafe();
				
				// Remove missing children from tracking list
				Map<String, WildChild> trackedCopy = new HashMap<String, WildChild>(trackedChildren);
				for(String trackedChild : trackedCopy.keySet()) {
					if(!children.contains(trackedChild)) {
						trackedChildren.remove(trackedChild);
						if(childIsLeaf) {
							WatchedEvent deletedEvent = new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path + "/" + trackedChild);
							leafWatcher.process(deletedEvent);
						}
					}
				}

				// Add new children to tracking list
				for(String child : children) {
					if(child.matches(matchCriteria) && !trackedChildren.containsKey(child)) {
						WildChild rcw = null;
						String childPath = path + "/" + child;
						if(childIsLeaf) {
							WatchedEvent createdEvent = new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, childPath);
							leafWatcher.process(createdEvent);
						} else {
							rcw = new WildChild(client, childPath, wildPath, leafWatcher);
						}
						trackedChildren.put(child, rcw);
					}
				}
			}
		}
	}
}
