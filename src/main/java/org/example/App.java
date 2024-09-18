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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;


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
            switch(Integer.parseInt(args[7])) {
                case 0:
                    bulkInsert(jsonObject, cluster, args);
                    break;
                case 1:
                    bulkInsertWithBuffer(jsonObject, cluster, args);
                    break;
                case 2:
                    bulkTransaction(jsonObject, cluster, args);
                    break;
                case 3:
                    bulkTransactionReactive(jsonObject, cluster, args, false);
                    break;
                case 4:
                    bulkTransactionWithBuffer(jsonObject, cluster, args);
                    break;
                case 5:
                    bulkTransactionReactiveWithBuffer(jsonObject, cluster, args);
                    break;
                case 6:
                    bulkTransactionWithMonoReactive(jsonObject, cluster, args);
                    break;
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println(duration / 1000000000 + "s");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static void bulkInsert(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors() * 8;
        var finalDocs = Integer.parseInt(args[3]);
        Flux.range(0, finalDocs)
                .parallel(concurrency)
                .concatMap(count -> {
                            if (count % 100 == 0) System.out.println(count);
                            return coll.insert(
                                    args[5] + "-" + count,
                                    jsonObject).onErrorComplete(DocumentExistsException.class).retry();
                        }
                )
                .then()
                .retry()
                .block();
    }

    public static void bulkInsertWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors() * 8;
        var buffer = Integer.parseInt(args[6]);
        var finalDocs = Integer.parseInt(args[3]);
        Flux.range(0, finalDocs)
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .concatMap(count -> {
                                    if (count % 100 == 0) System.out.println(count);
                                    return coll.insert(
                                            args[5] + "-" + count,
                                            jsonObject).onErrorComplete(DocumentExistsException.class).retry();
                                }
                        )
                        .then()
                        .retry()
                        .block()
                )
                .then()
                .retry()
                .block();
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
        int concurrency = Runtime.getRuntime().availableProcessors() * 8;
        AtomicInteger inFlight = new AtomicInteger(0);
        JsonObject jsonObject1 = JsonObject.create();

        TransactionResult result = cluster.reactive().transactions().run((ctx) -> Flux.range(0, num)
                .parallel(concurrency)
                .runOn(Schedulers.boundedElastic())
                .concatMap(
                        docId -> {
                            int x = inFlight.incrementAndGet();
                            if (docId % 50 == 0) System.out.println("Ferrari: " + docId + " " + x);
                            return ctx.insert(coll, docId.toString(), jsonObject1)
                                    .doOnNext(v -> inFlight.decrementAndGet());
                        }
                ).then(), TransactionOptions.transactionOptions().timeout(Duration.ofMinutes(10))).block();
//
//        int concurrency = Runtime.getRuntime().availableProcessors() * 8; // This many operations will be in-flight at once
//
//        Long start = System.nanoTime();
//        TransactionResult result = cluster.reactive().transactions().run((ctx) -> Flux.range(0, num)
//                .parallel(concurrency)
//                .runOn(Schedulers.boundedElastic())
//                .concatMap(
//                        docId -> {
//                            if (docId % 1000 == 0) {System.out.println(docId); System.out.println("millisecs " + (System.nanoTime() - start) / 1000000);}
//                            return ctx.insert(coll, docId.toString(), jsonObject);
//
//                        }
//                ).then()
//        ).doOnError(err -> {
//            if (err instanceof TransactionCommitAmbiguousException) {
//                throw logCommitAmbiguousError((TransactionCommitAmbiguousException) err);
//            } else if (err instanceof TransactionFailedException) {
//                throw logFailure((TransactionFailedException) err);
//            }
//        }).block();
//        try {
//            logger.addHandler(new FileHandler("./transactionlog.txt"));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        result.logs().forEach(message -> logger.info(message.toString()));
    }

    public static void bulkTransactionReactiveWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection("test");

        int concurrency = Runtime.getRuntime().availableProcessors() * 8; // This many operations will be in-flight at once

        int buffer = Integer.parseInt(args[6]);

        cluster.reactive().transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .runOn(Schedulers.boundedElastic())
                        .concatMap(docId -> {
                                    if (docId % 1000 == 0) System.out.println(docId);
//                                    return ctx.get(coll, docId.toString())
//                                            .doOnSuccess(doc -> ctx.replace(doc, jsonObject))
//                                            .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, docId.toString(), jsonObject));
                                    return ctx.insert(coll, docId.toString(), jsonObject);
                                }
                        )
                        .then()
                        .retry()
                        .block()
                )
                .then()
                .retry()
        ).then().retry().block();
    }

    public static void bulkTransaction(JsonObject jsonObject, Cluster cluster, String[] args) {

        Bucket bucket = cluster.bucket("test");
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Collection coll = bucket.scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors() * 8; // This many operations will be in-flight at once


        cluster.transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .parallel(concurrency)
                .runOn(Schedulers.boundedElastic())
                .map(
                        docId -> {
                            if (docId % 1000 == 0) System.out.println(docId);
                            try {
                                var doc = ctx.get(coll, docId.toString());
                                return ctx.replace(doc, jsonObject);
                            } catch (DocumentNotFoundException e) {
                                return ctx.insert(coll, docId.toString(), jsonObject);
                            }
                        }
                )
                .then()
                .retry()
                .block()
        );
    }

    public static void bulkTransactionWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {

        Bucket bucket = cluster.bucket("test");
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Collection coll = bucket.scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors() * 8; // This many operations will be in-flight at once
        int buffer = Integer.parseInt(args[6]);

        cluster.transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .map(docId -> {
                                    if (docId % 1000 == 0) System.out.println(docId);
                                    try {
                                        var doc = ctx.get(coll, docId.toString());
                                        return ctx.replace(doc, jsonObject);
                                    } catch (DocumentNotFoundException e) {
                                        return ctx.insert(coll, docId.toString(), jsonObject);
                                    }
                                }
                        )
                        .then()
                        .retry()
                        .block()
                )
                .then()
                .retry()
                .block()
        );
    }

    public static void bulkTransactionWithMonoReactive(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection("test");
        cluster.reactive().transactions().run((ctx) -> {
                    List<Mono<TransactionGetResult>> monoList = new ArrayList<>();
                    for (int i = 0; i <= Integer.parseInt(args[3]); i++) {
                        int finalI = i;
                        monoList.add(ctx.get(coll, String.valueOf(i))
                                .doOnSuccess(doc -> ctx.replace(doc, jsonObject))
                                .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, String.valueOf(finalI), jsonObject))
                                .doAfterTerminate(() -> {
                                            if (finalI % 100 == 0)
                                                System.out.println(finalI);
                                        }
                                ));
                    }

                    Flux<TransactionGetResult> flux = Flux.concat(monoList);
                    return Mono.from(flux.collectList().then());
                }
        ).then().retry().block();

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