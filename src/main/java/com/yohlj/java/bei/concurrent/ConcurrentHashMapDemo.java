package com.yohlj.java.bei.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@RestController
@RequestMapping("/concurrenthashmap")
public class ConcurrentHashMapDemo {

    // 线程个数
    private static final int THREAD_COUNT = 10;
    // 总元素数量
    private static final int ITEM_COUNT = 1000;

    private final Logger log = LoggerFactory.getLogger(ConcurrentHashMapDemo.class);

    // 帮助方法，用来获得一个指定元素数量模拟数据的 ConcurrentHashMap
    private ConcurrentHashMap<String, Long> getData(int count) {
        return LongStream.rangeClosed(1, count)
                .boxed()
                .collect(Collectors.toConcurrentMap(i -> UUID.randomUUID().toString(), Function.identity(),
                        (o1, o2) -> o1, ConcurrentHashMap::new));
    }

    /*
     ConcurrentHashMap 只能保证提供的原子性读写操作是线程安全的
     */

    @GetMapping("/wrong")
    public String wrong() throws InterruptedException {
        ConcurrentHashMap<String, Long> concurrentHashMap = getData(ITEM_COUNT - 100);
        // 初始 900 个元素
        log.info("init size:{}", concurrentHashMap.size());
        ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT);
        // 使用线程池并发处理逻辑
        forkJoinPool.execute(() -> IntStream.rangeClosed(1, 10)
                .parallel()
                .forEach(i -> {
                    // 查询还需要补充多少个元素
                    int gap = ITEM_COUNT - concurrentHashMap.size();
                    log.info("gap size:{}", gap);
                    // 补充元素
                    concurrentHashMap.putAll(getData(gap));
                }));
        // 等待所有任务完成
        forkJoinPool.shutdown();
        forkJoinPool.awaitTermination(1, TimeUnit.HOURS);
        // 最后元素个数会是 1000 吗？
        log.info("finish size:{}", concurrentHashMap.size());
        return "OK";
    }

    /*
    需要注意 ConcurrentHashMap 对外提供的方法或能力的限制：

    使用了 ConcurrentHashMap，不代表对它的多个操作之间的状态是一致的，是没有其他线程在操作它的，如果需要确保需要手动加锁。
    诸如 size、isEmpty 和 containsValue 等聚合方法，在并发情况下可能会反映 ConcurrentHashMap 的中间状态。因此在并发情况下，
        这些方法的返回值只能用作参考，而不能用于流程控制。显然，利用 size 方法计算差异值，是一个流程控制。
    诸如 putAll 这样的聚合方法也不能确保原子性，在 putAll 的过程中去获取数据可能会获取到部分数据。
     */

    @GetMapping("/right")
    public String right() throws InterruptedException {
        ConcurrentHashMap<String, Long> concurrentHashMap = getData(ITEM_COUNT - 100);
        // 初始 900 个元素
        log.info("init size:{}", concurrentHashMap.size());
        ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT);
        // 使用线程池并发处理逻辑
        forkJoinPool.execute(() -> IntStream.rangeClosed(1, 10)
                .parallel()
                .forEach(i -> {
                    // 下面的这段复合逻辑需要锁一下这个 ConcurrentHashMap
                    synchronized (concurrentHashMap) {
                        // 查询还需要补充多少个元素
                        int gap = ITEM_COUNT - concurrentHashMap.size();
                        log.info("gap size:{}", gap);
                        // 补充元素
                        concurrentHashMap.putAll(getData(gap));
                    }
                }));
        // 等待所有任务完成
        forkJoinPool.shutdown();
        forkJoinPool.awaitTermination(1, TimeUnit.HOURS);
        // 最后元素个数会是 1000 吗？
        log.info("finish size:{}", concurrentHashMap.size());
        return "OK";
    }

    /*
    没有充分了解并发工具的特性，从而无法发挥其威力
     */

    // 循环次数
    private static final int LOOP_COUNT = 10000000;

    private Map<String, Long> normaluse() throws InterruptedException {
        ConcurrentHashMap<String, Long> freqs = new ConcurrentHashMap<>(ITEM_COUNT);
        ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT);
        forkJoinPool.execute(() -> IntStream.rangeClosed(1, LOOP_COUNT)
                .parallel()
                .forEach(i -> {
                            // 获得一个随机的 Key
                            String key = "item" + ThreadLocalRandom.current().nextInt(ITEM_COUNT);
                            synchronized (freqs) {
                                if (freqs.containsKey(key)) {
                                    // Key 存在则 +1
                                    freqs.put(key, freqs.get(key) + 1);
                                } else {
                                    // Key 不存在则初始化为 1
                                    freqs.put(key, 1L);
                                }
                            }
                        }
                ));
        forkJoinPool.shutdown();
        forkJoinPool.awaitTermination(1, TimeUnit.HOURS);
        return freqs;
    }

    /*
    使用 ConcurrentHashMap 的原子性方法 computeIfAbsent 来做复合逻辑操作，判断 Key 是否存在 Value，
        如果不存在则把 Lambda 表达式运行后的结果放入 Map 作为 Value，也就是新创建一个 LongAdder 对象，最后返回 Value。

    由于 computeIfAbsent 方法返回的 Value 是 LongAdder，是一个线程安全的累加器，因此可以直接调用其 increment 方法进行累加。


     */

    private Map<String, Long> gooduse() throws InterruptedException {
        ConcurrentHashMap<String, LongAdder> freqs = new ConcurrentHashMap<>(ITEM_COUNT);
        ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT);
        forkJoinPool.execute(() -> IntStream.rangeClosed(1, LOOP_COUNT)
                .parallel()
                .forEach(i -> {
                            String key = "item" + ThreadLocalRandom.current().nextInt(ITEM_COUNT);
                            // 利用 computeIfAbsent() 方法来实例化 LongAdder，然后利用 LongAdder来进行
                            freqs.computeIfAbsent(key, k -> new LongAdder()).increment();
                        }
                ));
        forkJoinPool.shutdown();
        forkJoinPool.awaitTermination(1, TimeUnit.HOURS);
        // 因为我们的 Value 是 LongAdder 而不是 Long，所以需要做一次转换才能返回
        return freqs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue()));
    }

    @GetMapping("good")
    public String good() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("normaluse");
        Map<String, Long> normaluse = normaluse();
        stopWatch.stop();
        // 校验元素数量
        Assert.isTrue(normaluse.size() == ITEM_COUNT, "normaluse size error");
        // 校验累计总数
        Assert.isTrue(normaluse.values().stream()
                        .mapToLong(aLong -> aLong)
                        .reduce(0, Long::sum) == LOOP_COUNT
                , "normaluse count error");
        stopWatch.start("gooduse");
        Map<String, Long> gooduse = gooduse();
        stopWatch.stop();
        Assert.isTrue(gooduse.size() == ITEM_COUNT, "gooduse size error");
        Assert.isTrue(gooduse.values().stream()
                        .mapToLong(aLong -> aLong)
                        .reduce(0, Long::sum) == LOOP_COUNT
                , "gooduse count error");
        log.info(stopWatch.prettyPrint());
        return "OK";
    }
}
