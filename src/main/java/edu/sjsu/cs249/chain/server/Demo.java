package edu.sjsu.cs249.chain.server;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Demo {

    AtomicBoolean retryChainCheck = new AtomicBoolean(true);
    public Demo() {
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {
            System.out.println("check:" + Thread.currentThread().getName());
            if (retryChainCheck.getAndSet(false)) {
                System.out.println("nono");
            }
            }, 5, 5, TimeUnit.MILLISECONDS);
    }

    public void checkHeadTail() {
        System.out.println("Hello");
    }

    public void print() {
        System.out.println("hello there");
    }
    public static void main(String[] args) {
          Demo demo = new Demo();
          System.out.println("Demo:" + Thread.currentThread().getName());
          System.out.println("I'm exiting...");
    }
}
