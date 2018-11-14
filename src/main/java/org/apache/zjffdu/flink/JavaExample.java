package org.apache.zjffdu.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JavaExample {

    static ExecutorService executor = Executors.newFixedThreadPool(3, new ThreadFactory() {
        int count = 1;
        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "custom-executor-" + count++);
        }
    });


    public static void main(String[] args) throws ExecutionException, InterruptedException {

    }
}
