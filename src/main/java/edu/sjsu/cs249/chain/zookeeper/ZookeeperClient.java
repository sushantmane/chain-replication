package edu.sjsu.cs249.chain.zookeeper;

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
    private static final int TIMEOUT = 30000;

    private ZooKeeper zk;
    private String connectString;
    private long mySid;
    private String root;
    private CountDownLatch connLatch = new CountDownLatch(1);
    private AtomicLong predecessorSid = new AtomicLong(-1);
    private AtomicLong successorSid = new AtomicLong(-1);
    private AtomicLong headSid = new AtomicLong(-1);
    private AtomicLong tailSid = new AtomicLong(-1);

    public ZookeeperClient(String connectString, String root) {
        this.connectString = connectString;
        this.root =  root;
    }

    public String getRoot() {
        return root;
    }

    // connect to zookeeper server and establish a session
    public void connect() throws IOException, InterruptedException {
        LOG.info("Connecting to zookeeper server...");
        zk = new ZooKeeper(connectString, TIMEOUT,this);
        // wait for session establishment
        connLatch.await();
        mySid = zk.getSessionId();
        LOG.info("Connected to zookeeper server. SessionId: {}", mySid);
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
    public void process(WatchedEvent event) {
        LOG.debug("New notification - type: {} state: {}", event.getType(),
                event.getState().toString());
        if (event.getState() == Event.KeeperState.Expired
                || event.getState() == Event.KeeperState.Closed) {
            LOG.info("Zookeeper session expired/closed. Cause: {}", event.getState().toString());
            // exit on session expiration
            System.exit(2);
        }
        if (mySid == 0 && event.getState() == Event.KeeperState.SyncConnected) {
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
    public void updateContext() throws KeeperException, InterruptedException {
        // todo: replace with chain root
        List<Long> sids = getOrderListOfChainNodes("/tail-chain", true);
        if (sids.size() == 0) {
            return;
        }
        headSid.set(sids.get(0));
        tailSid.set(sids.get(sids.size() - 1));
        // position of this replica in chain
        int myIndex = sids.indexOf(mySid);
        // if client is calling this method
        if (myIndex == -1) {
            return;
        }
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

    // create a ephemeral node for given sessionId and set data for it
    public String createEphZnode(long sessionId, String data)
            throws KeeperException, InterruptedException {
        String node = root + "/" + Utils.getHexSid(zk.getSessionId());
        return create(node, data, CreateMode.EPHEMERAL);
    }

    // get data of the node
    public byte[] getData(String path, boolean setWatch)
            throws KeeperException, InterruptedException {
        return zk.getData(path, setWatch, null);
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
    public boolean isSuccessor(long sid) {
        return sid == successorSid.get();
    }

    // return true of sid is predecessor of this session id
    public boolean isPredecessor(long sid) {
        return sid == predecessorSid.get();
    }

    public boolean isHead(long sid) {
        return sid == headSid.get();
    }

    public boolean isTail(long sid) {
        return sid == tailSid.get();
    }

    public String getHeadReplica() {
        return Utils.getHexSid(headSid.get());
    }

    public String getTailReplica() {
        return Utils.getHexSid(tailSid.get());
    }

    // todo: for following two methods, data should be refreshed by updateContext()
    public long getSuccessor(String path, long sid) throws KeeperException, InterruptedException {
        List<Long> replicas = getOrderListOfChainNodes(path, true);
        int index = replicas.indexOf(sid);
        if (index == -1 || index == replicas.size() - 1) {
            return -1;
        }
        return replicas.get(index + 1);
    }

    public long getPredecessor(String path, long sid) throws KeeperException, InterruptedException {
        List<Long> replicas = getOrderListOfChainNodes(path, true);
        int index = replicas.indexOf(sid);
        if (index <= 0) {
            return -1;
        }
        return replicas.get(index - 1);
    }
}
