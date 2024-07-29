package com.eurotech.cloud.elastic.rest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EsPerformanceEvaluator {

    public static void main(String[] args) throws Exception {
        EsPerformanceEvaluator main = new EsPerformanceEvaluator();
        main.runPerformanceTest();
    }

    private final Random rand = new Random();

    void runPerformanceTest() {
        int repetitions = 2; //odd numbers
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        System.out.println(String.format("Available processors: %2d, Attempts per combination: %2d", availableProcessors, repetitions));

        final int resolution = availableProcessors;
        final List<Integer> insertions = IntStream.rangeClosed(0, availableProcessors)
                .filter(i -> i % resolution == 1 || i == availableProcessors)
                .mapToObj(i -> i * 1000)
                .collect(Collectors.toList());
        final List<Integer> clientPoolSize = IntStream.rangeClosed(0, availableProcessors)
                .filter(i -> i % resolution == 1 || i == availableProcessors)
                .mapToObj(i -> i)
                .collect(Collectors.toList());
        final List<Integer> executorThreads = IntStream.rangeClosed(0, availableProcessors)
                .filter(i -> i % resolution == 1 || i == availableProcessors)
                .mapToObj(i -> i * 10)
                .collect(Collectors.toList());
        final List<Integer> threadsPerClient = IntStream.rangeClosed(0, availableProcessors)
                .filter(i -> i % resolution == 1 || i == availableProcessors)
                .mapToObj(i -> i)
                .collect(Collectors.toList());

        insertions.forEach(numberOfInsertions -> {
            clientPoolSize.forEach(finalClientPoolSize -> {
                executorThreads.forEach(executorThreadsCount -> {
                    threadsPerClient.forEach(threadSize -> {
                        collectAndPrintStats(i -> new HighLevelEsExecutor(threadSize), "High", numberOfInsertions, finalClientPoolSize, executorThreadsCount, threadSize, repetitions,
                                finalClientPoolSize);
                        collectAndPrintStats(i -> new LowLevelEsExecutor(threadSize), "Low ", numberOfInsertions, finalClientPoolSize, executorThreadsCount, threadSize, repetitions,
                                finalClientPoolSize);
                    });
                });
            });
        });
    }

    private void collectAndPrintStats(IntFunction<EsExecutor> clientBuilder, String clientType, Integer numberOfInsertions, Integer finalClientPoolSize, Integer executorThreadsCount,
            Integer threadSize,
            int repetitions, Integer clientPoolSize) {
        //Print row head
        System.out.print(String.format(
                "Testing %s level with poolsize %2d, executor threads %3d, threads per client %2d, %5d insertions, time: ",
                clientType,
                clientPoolSize,
                executorThreadsCount,
                threadSize,
                numberOfInsertions));

        //Perform repetitions and collect stats
        final LongSummaryStatistics stats = IntStream.rangeClosed(1, repetitions)
                .mapToLong(i -> {
                    final List<EsExecutor> highLevelEsExecutors = IntStream.rangeClosed(1, finalClientPoolSize)
                            .mapToObj(clientBuilder)
                            .collect(Collectors.toList());
                    return measureTime(highLevelEsExecutors, executorThreadsCount, threadSize, numberOfInsertions);
                })
                .summaryStatistics();

        //print out stats
        System.out.println(
                String.format("avg: %5d ms, min: %5d ms, max:%5d ms, (x mex) avg: %5d ns, min: %5d ns, max:%5d ns",
                        Double.valueOf(stats.getAverage()).longValue(), stats.getMin(), stats.getMax(),
                        Double.valueOf(stats.getAverage() / numberOfInsertions * 1000).longValue(), stats.getMin() * 1000 / numberOfInsertions,
                        stats.getMax() * 1000 / numberOfInsertions));
    }

    public long measureTime(List<EsExecutor> executors, int executorThreadsCount, int threadCountPerClient, int numberOfInsertions) {
        final ExecutorService executorSvc = Executors.newFixedThreadPool(executorThreadsCount);
        executors.get(0).dropAndRecreateIndex();
        try {
            final CountDownLatch executedRequests = new CountDownLatch(numberOfInsertions);
            Instant start_time = Instant.now();
            for (int j = 0; j < numberOfInsertions; j++) {
                //Uncomment and adjust this to understand how much of a difference your tuning does, when your business logic is already orders of magnitude slower anyway
                //                Thread.sleep(1);
                executorSvc.submit(() -> {
                    Instant request_start = Instant.now();
                    int selector = -1;
                    try {
                        selector = rand.nextInt(executors.size());
                        final EsExecutor esExecutor = executors.get(selector);
                        esExecutor.performIndexing();
                        executedRequests.countDown();
                        Duration dur = Duration.between(request_start, Instant.now());
                    } catch (Exception e) {
                        executedRequests.countDown();
                        if (e.getMessage() != null) {
                            System.out.println("On failure, Client: " + selector + ", Thread: " + Thread.currentThread().getId() + ", Duration: "
                                    + Duration.between(request_start, Instant.now()) + ", Error: " + e.getMessage());
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
            }
            executedRequests.await();
            final Duration executionTime = Duration.between(start_time, Instant.now());
            return executionTime.toMillis();
        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            for (int j = 0; j < executors.size(); j++) {
                executors.get(j).close();
            }
            executorSvc.shutdown();
            try {
                executorSvc.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return 0;
    }
}
