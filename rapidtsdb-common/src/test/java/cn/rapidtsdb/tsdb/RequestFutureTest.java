package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.common.protonetty.RequestFuture;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Log4j2
public class RequestFutureTest {

    public static void main(String[] args) {
        System.out.println("Start");
        ExecutorService executor = Executors.newCachedThreadPool();
        RequestFuture<String> requestFuture = new RequestFuture(123);
        requestFuture.setResult("Smith");
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                System.out.printf("%s start block get\n", Thread.currentThread().getName());
                String name = requestFuture.block();
                System.out.printf("%s return  %s\n", Thread.currentThread().getName(), name);
            });
        }
        executor.submit(()->{
            System.out.printf("start waiting 5 secons\n");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("setting");
            requestFuture.setResult("Hello");
        });
    }

}
