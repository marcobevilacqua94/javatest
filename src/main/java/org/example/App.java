package org.example;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * Hello world!
 */
public class App {
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


        JsonObject jsonObject = JsonObject.create();

        jsonObject.put("body", RandomString(Integer.parseInt(args[4])));

        long startTime = System.nanoTime();
        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])
                        .environment(env -> env.transactionsConfig(TransactionsConfig.builder()
                                .timeout(Duration.ofMinutes(10))
                                .durabilityLevel(DurabilityLevel.NONE)
                                .build()))
        )

        ) {

            switch(Integer.parseInt(args[6])) {
                case 0:
                    bulkInsert(jsonObject, cluster, args);
                case 1:
                    bulkInsertWithBuffer(jsonObject, cluster, args);
                case 2:
                    bulkTransaction(jsonObject, cluster, args);
                case 3:
                    bulkTransactionReactive(jsonObject, cluster, args);
                case 4:
                    bulkTransactionWithBuffer(jsonObject, cluster, args);
                case 5:
                    bulkTransactionReactiveWithBuffer(jsonObject, cluster, args);
                case 6:
                    bulkTransactionWithMonoReactive(jsonObject, cluster, args);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println(duration / 1000000000 + "s");
        }
    }


    public static void bulkInsert(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveCollection coll = cluster.bucket("test").scope("test").collection("test").reactive();
        int concurrency = Runtime.getRuntime().availableProcessors();
        var finalDocs = Integer.parseInt(args[3]);
        Flux.range(0, finalDocs)
                .parallel(concurrency)
                .flatMap(count -> {
                            if (count % 100 == 0) System.out.println(count);
                            return coll.insert(
                                    args[5] + "-" + count,
                                    jsonObject).onErrorComplete(DocumentExistsException.class).retry();
                        }
                )
                .sequential()
                .collectList()
                .block();
    }

    public static void bulkInsertWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveCollection coll = cluster.bucket("test").scope("test").collection("test").reactive();
        int concurrency = Runtime.getRuntime().availableProcessors();
        var buffer = 30000;
        var finalDocs = Integer.parseInt(args[3]);
        Flux.range(0, finalDocs)
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .flatMap(count -> {
                                    if (count % 100 == 0) System.out.println(count);
                                    return coll.insert(
                                            args[5] + "-" + count,
                                            jsonObject).onErrorComplete(DocumentExistsException.class).retry();
                                }
                        )
                        .sequential()
                        .retry()
                        .collectList()
                        .block()
                ).retry()
                .collectList()
                .block();
    }

    public static void bulkTransactionReactive(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveCollection coll = cluster.bucket("test").scope("test").collection("test").reactive();

        int concurrency = Runtime.getRuntime().availableProcessors(); // This many operations will be in-flight at once


        cluster.reactive().transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .parallel(concurrency)
                .runOn(Schedulers.boundedElastic())
                .flatMap(
                        docId -> {
                            if (docId % 100 == 0) System.out.println(docId);
                            return ctx.get(coll, docId.toString())
                                    .doOnSuccess(doc -> ctx.replace(doc, jsonObject))
                                    .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, docId.toString(), jsonObject));
                        }
                ).then()
        ).then().block();
    }

    public static void bulkTransactionReactiveWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveCollection coll = cluster.bucket("test").scope("test").collection("test").reactive();

        int concurrency = Runtime.getRuntime().availableProcessors(); // This many operations will be in-flight at once

        int buffer = 100;

        cluster.reactive().transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .flatMap(docId -> {
                                    if (docId % 100 == 0) System.out.println(docId);
                                    return ctx.get(coll, docId.toString())
                                            .doOnSuccess(doc -> ctx.replace(doc, jsonObject))
                                            .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, docId.toString(), jsonObject));
                                }
                        )
                        .sequential()
                        .retry()
                        .collectList()
                        .block()
                ).retry()
                .collectList()
        ).then().block();
    }

    public static void bulkTransaction(JsonObject jsonObject, Cluster cluster, String[] args) {

        Collection coll = cluster.bucket("test").scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors(); // This many operations will be in-flight at once


        cluster.transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .parallel(concurrency)
                .runOn(Schedulers.boundedElastic())
                .map(
                        docId -> {
                            if (docId % 100 == 0) System.out.println(docId);
                            try {
                                var doc = ctx.get(coll, docId.toString());
                                return ctx.replace(doc, jsonObject);
                            } catch (DocumentNotFoundException e) {
                                return ctx.insert(coll, docId.toString(), jsonObject);
                            }
                        }
                )
                .then()
                .block()
        );
    }

    public static void bulkTransactionWithBuffer(JsonObject jsonObject, Cluster cluster, String[] args) {

        Collection coll = cluster.bucket("test").scope("test").collection("test");
        int concurrency = Runtime.getRuntime().availableProcessors(); // This many operations will be in-flight at once
        int buffer = 1000;

        cluster.transactions().run((ctx) -> Flux.range(0, Integer.parseInt(args[3]))
                .buffer(buffer)
                .map(countList -> Flux.fromIterable(countList)
                        .parallel(concurrency)
                        .map(docId -> {
                                    if (docId % 100 == 0) System.out.println(docId);
                                    try {
                                        var doc = ctx.get(coll, docId.toString());
                                        return ctx.replace(doc, jsonObject);
                                    } catch (DocumentNotFoundException e) {
                                        return ctx.insert(coll, docId.toString(), jsonObject);
                                    }
                                }
                        )
                        .sequential()
                        .retry()
                        .collectList()
                        .block()
                ).retry()
                .collectList()
                .block()
        );
    }

    public static void bulkTransactionWithMonoReactive(JsonObject jsonObject, Cluster cluster, String[] args) {
        ReactiveCollection coll = cluster.bucket("test").scope("test").collection("test").reactive();
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
        ).then().block();

    }

}
