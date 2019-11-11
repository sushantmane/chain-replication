package edu.sjsu.cs249.chain.client;

public class Response {

    enum Code {
        SUCCESS, // operations successful
        ECHNMTY,  // chain empty
        ENOKEY,  // no such key
        EFAULT,   // something went wrong
    }

    private String key ;
    private int value;
    private Code rc;

    public Response(String key, int value, Code rc) {
        this.key = key;
        this.value = value;
        this.rc = rc;
    }

    public Response(String key, Code rc) {
        this.key = key;
        this.rc = rc;
    }

    private Response() {}

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public Code getRc() {
        return rc;
    }
}
