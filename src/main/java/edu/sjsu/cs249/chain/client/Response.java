package edu.sjsu.cs249.chain.client;

public class Response {

    public enum Code {
        SUCCESS,    // operations successful
        ECHNMTY,    // chain empty
        ENOKEY,     // no such key
        EFAULT,     // something went wrong
        EABORT,     // request aborted
    }

    private String key ;
    private int value;
    private Code rc;

    public Response(Code rc, String key, int value) {
        this.rc = rc;
        this.key = key;
        this.value = value;
    }

    public Response(Code rc, String key) {
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
