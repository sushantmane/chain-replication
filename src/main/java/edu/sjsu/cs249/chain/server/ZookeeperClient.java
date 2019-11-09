package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ZookeeperClient implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);

    private ZooKeeper zk;
    private String connectString;
    private int timeout;
    private long mySid;
    private CountDownLatch connLatch = new CountDownLatch(1);
    private AtomicLong predecessorSid = new AtomicLong();
    private AtomicLong successorSid = new AtomicLong();
    private AtomicLong headSid = new AtomicLong();
    private AtomicLong tailSid = new AtomicLong();

    protected ZookeeperClient(String connectString, int timeout) {
        this.connectString = connectString;
        this.timeout = timeout;
    }

    // connect with zookeeper server and establish a session
    // return -1 if unable to establish a session otherwise return session id
    public long connect() {
        try {
            LOG.info("Connecting to zookeeper server...");
            zk = new ZooKeeper(connectString, timeout,this);
            // wait for session establishment
            connLatch.await();
            mySid = zk.getSessionId();
            LOG.info("Connected to zookeeper server. SessionId: {}", mySid);
        } catch (IOException | InterruptedException e) {
            return -1;
        }
        return mySid;
    }

    public long getSessionId() {
        return mySid;
    }

    // end zookeeper session
    public void closeConnection() {
        try {
            zk.close();
            LOG.info("Zookeeper connection has been closed.");
        } catch (InterruptedException e) {
            // ok
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("New notification - type: {} state: {}", watchedEvent.getType(),
                watchedEvent.getState().toString());
        if (watchedEvent.getState() == Event.KeeperState.Expired
                || watchedEvent.getState() == Event.KeeperState.Closed) {
            LOG.info("Stopping zookeeper. Cause: " + watchedEvent.getState().toString());
        }
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            // session established, open connect latch
            connLatch.countDown();
        }
        try {
            updateContext();
        } catch (KeeperException | InterruptedException e) {
            // failed to update context; retry
        }
    }

    // update roles of nodes in chain
    protected void updateContext() throws KeeperException, InterruptedException {
        // todo: replace with chain root
        List<Long> sids = getOrderListOfChainNodes("/tail-chain", true);
        if (sids.size() == 0) {
            return;
        }
        headSid.set(sids.get(0));
        tailSid.set(sids.get(sids.size() - 1));
        // position of this replica in chain
        int myIndex = sids.indexOf(mySid);
        // except head replica all other nodes have predecessor replica
        if (myIndex == 0) {
            predecessorSid.set(-1); // -1 indicates no predecessor
        } else {
            predecessorSid.set(sids.get(myIndex - 1));
        }
        // except tail replica all other nodes have successor replica
        if (myIndex == sids.size() - 1) {
            successorSid.set(-1); // -1 indicates no successor
        } else {
            successorSid.set(sids.get(myIndex + 1));
        }
    }

    // return true if node exists
    public boolean exists(String path, boolean watch)
            throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, watch);
        if (stat == null) {
            return false;
        }
        return true;
    }

    // create a node and set data for it
    public String create(String node, String data, CreateMode mode)
            throws KeeperException, InterruptedException {
        return zk.create(node, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    // get data of the node
    public Stat getData(String path, boolean setWatch)
            throws KeeperException, InterruptedException {
        Stat data = new Stat();
        zk.getData(path, setWatch, data);
        return data;
    }

    // get children of the node represented by path
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch);
    }

    // delete the node
    public int delete(String path) {
        int rc = 0; // OK
        try {
            zk.delete(path, -1);
        } catch (KeeperException e) {
           if (e.code() == KeeperException.Code.NONODE) {
               rc = 2; // ENOENT
           }
        } catch (InterruptedException e) {
            rc = 11; // EAGAIN
        }
        return rc;
    }

    // returns sorted list of children of given node
    protected List<Long> getOrderListOfChainNodes(String path, boolean watch)
            throws KeeperException, InterruptedException {
        List<String> children = getChildren(path, watch);
        List<Long> sessionIds = new ArrayList<>();
        for (String child : children) {
            try {
                sessionIds.add(Utils.getLongSid(child));
            } catch (NumberFormatException e) {
                // skip non-numbers
            }
        }
        sessionIds.sort(Long::compare);
        return sessionIds;
    }

    // return true of sid is successor of this session id
    protected boolean isSuccessor(long sid) {
        return sid == successorSid.get();
    }

    // return true of sid is predecessor of this session id
    protected boolean isPredecessor(long sid) {
        return sid == predecessorSid.get();
    }

    protected boolean isHead(long sid) {
        return sid == headSid.get();
    }

    protected boolean isTail(long sid) {
        return sid == tailSid.get();
    }

    protected boolean amIHead() {
        return mySid == headSid.get();
    }

    protected boolean amITail() {
        return mySid == tailSid.get();
    }

    // todo: remove following two methods
    protected long getNext(String path, long sid) throws KeeperException, InterruptedException {
        List<Long> replicas = getOrderListOfChainNodes(path, true);
        int index = replicas.indexOf(sid);
        if (index == -1 || index == replicas.size() - 1) {
            return -1;
        }
        return replicas.get(index + 1);
    }

    protected long getPrev(String path, long sid) throws KeeperException, InterruptedException {
        List<Long> replicas = getOrderListOfChainNodes(path, true);
        int index = replicas.indexOf(sid);
        if (index <= 0) {
            return -1;
        }
        return replicas.get(index - 1);
    }
}
