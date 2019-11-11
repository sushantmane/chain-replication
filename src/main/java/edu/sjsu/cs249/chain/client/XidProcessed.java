package edu.sjsu.cs249.chain.client;

public class XidProcessed {
    private int xid;

    XidProcessed(int xid) {
        this.xid = xid;
    }

    private XidProcessed() {}

    public int getXid() {
        return xid;
    }
}
