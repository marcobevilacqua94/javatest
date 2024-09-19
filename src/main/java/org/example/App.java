package org.example;

import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

import java.io.FileNotFoundException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.io.PrintWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.io.File;

import static java.lang.Integer.valueOf;

/**
 * Hello world!
 */
public class App {
    static Logger logger = LogManager.getLogManager().getLogger("global");
    private static String RandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = (int) (Math.random() * characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    public static void main(String[] args) {

        System.out.println("Hello World!");

        try {
            logger.addHandler(new FileHandler("./logs.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        JsonObject jsonObject = JsonObject.create();

        jsonObject.put("body", RandomString(Integer.parseInt(args[4])));


        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])

                        .environment(env -> {env.transactionsConfig(TransactionsConfig.builder()
                                .timeout(Duration.ofMinutes(10))

                                .durabilityLevel(DurabilityLevel.NONE)
                                .build()); env.ioConfig().numKvConnections(64);})
        )

        ) {
            bulkTransactionReactive(jsonObject, cluster, args, true);
            Thread.sleep(5000);
            long startTime = System.nanoTime();

            bulkTransactionReactive(jsonObject, cluster, args, false);


            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println(duration / 1000000000 + "s");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }



    public static void bulkTransactionReactive(JsonObject jsonObject, Cluster cluster, String[] args, boolean warmup) {

        String collectionName = "test";
        int num;
        if (warmup) {
            collectionName = "warmup";
            num = 500;
        } else {
            num = Integer.parseInt(args[3]);
        }
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection(collectionName);

        int concurrency = Runtime.getRuntime().availableProcessors() * 8;

        TransactionResult result = cluster.reactive().transactions().run((ctx) -> {

                    Mono<Void> firstOp = ctx.insert(coll, "0", jsonObject).then();

                    Mono<Void> restOfOps = Flux.range(1, new Integer(num))
                            .parallel(concurrency)
                            .runOn(Schedulers.newBoundedElastic(concurrency, Integer.MAX_VALUE, "bounded"))
                            .concatMap(
                                    docId -> ctx.insert(coll, docId.toString(), jsonObject)
                            ).sequential().then();


                    return firstOp.then(restOfOps);

                }, TransactionOptions.transactionOptions().timeout(Duration.ofMinutes(10))
        ).doOnError(err -> {
            logger.info("Transaction failed");
        }).block();
        if (warmup)
            logger.info("Warmup transaction completed");
        else
            logger.info("Transaction completed");
        try (PrintWriter writer = new PrintWriter("logs_ExtParallelUnstaging.txt")) {
            result.logs().forEach(log -> writer.println(log));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
