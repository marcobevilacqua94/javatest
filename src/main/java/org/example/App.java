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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;
import java.util.logging.Logger;


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
        jsonObject.put("version", args[5]);


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
        if(warmup){
            collectionName = "warmup";
            num = 500;
        } else {
            num = Integer.parseInt(args[3]);
        }
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection(collectionName);
//        int concurrency = Runtime.getRuntime().availableProcessors() * 8;
//        AtomicInteger inFlight = new AtomicInteger(0);
//        JsonObject jsonObject1 = JsonObject.create();
//
//        TransactionResult result = cluster.reactive().transactions().run((ctx) -> Flux.range(0, num)
//                .parallel(concurrency)
//                .runOn(Schedulers.boundedElastic())
//                .concatMap(
//                        docId -> {
//                            int x = inFlight.incrementAndGet();
//                            if (docId % 50 == 0) System.out.println("Ferrari: " + docId + " " + x);
//                            return ctx.insert(coll, docId.toString(), jsonObject1)
//                                    .doOnNext(v -> inFlight.decrementAndGet());
//                        }
//                ).then(), TransactionOptions.transactionOptions().timeout(Duration.ofMinutes(10))).block();

        int concurrency = Runtime.getRuntime().availableProcessors() * 8; // This many operations will be in-flight at once

        Long start = System.nanoTime();
        TransactionResult result = cluster.reactive().transactions().run((ctx) -> Flux.range(0, num)
                .parallel(concurrency)
                .runOn(Schedulers.boundedElastic())
                .concatMap(
                        docId -> {
                            if (docId % 1000 == 0) {System.out.println(docId); System.out.println("millisecs " + (System.nanoTime() - start) / 1000000);}
                            return ctx.insert(coll, docId.toString(), jsonObject);

                        }
                ).then()
        ).doOnError(err -> {
            if (err instanceof TransactionCommitAmbiguousException) {
                throw logCommitAmbiguousError((TransactionCommitAmbiguousException) err);
            } else if (err instanceof TransactionFailedException) {
                throw logFailure((TransactionFailedException) err);
            }
        }).block();
        try {
            logger.addHandler(new FileHandler("./transactionlog.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        result.logs().forEach(message -> logger.info(message.toString()));
    }

    public static RuntimeException logCommitAmbiguousError(TransactionCommitAmbiguousException err) {
        // This example will intentionally not compile: the application needs to use
        // its own logging system to replace `logger`.
        logger.warning("Transaction possibly reached the commit point");

        for (TransactionLogEvent msg : err.logs()) {
            logger.warning(msg.toString());
        }

        return err;
    }

    public static RuntimeException logFailure(TransactionFailedException err) {
        logger.warning("Transaction did not reach commit point");

        for (TransactionLogEvent msg : err.logs()) {
            logger.warning(msg.toString());
        }

        return err;
    }

}