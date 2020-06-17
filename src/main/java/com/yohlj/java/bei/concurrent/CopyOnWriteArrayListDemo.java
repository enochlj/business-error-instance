package com.yohlj.java.bei.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/copyonwritearraylist")
public class CopyOnWriteArrayListDemo {

    private final Logger log = LoggerFactory.getLogger(CopyOnWriteArrayListDemo.class);

    /*
    在 Java 中，CopyOnWriteArrayList 虽然是一个线程安全的 ArrayList，但因为其实现方式是，每次修改数据时都会复制一份数据出来，
        所以有明显的适用场景，即读多写少或者说希望无锁读的场景。
     */

    // 测试并发写的性能
    @GetMapping("/write")
    public Map<String, Object> testWrite() {
        List<Integer> copyOnWriteArrayList = new CopyOnWriteArrayList<>();
        StopWatch stopWatch = new StopWatch();
        int loopCount = 100000;
        stopWatch.start("Write:copyOnWriteArrayList");
        // 循环 100000 次并发往 CopyOnWriteArrayList 写入随机元素
        IntStream.rangeClosed(1, loopCount)
                .parallel()
                .forEach(__ -> copyOnWriteArrayList.add(ThreadLocalRandom.current().nextInt(loopCount)));

        stopWatch.stop();

        List<Integer> synchronizedList = Collections.synchronizedList(new ArrayList<>());
        stopWatch.start("Write:synchronizedList");
        // 循环 100000 次并发往加锁的 ArrayList 写入随机元素
        IntStream.rangeClosed(1, loopCount)
                .parallel()
                .forEach(__ -> synchronizedList.add(ThreadLocalRandom.current().nextInt(loopCount)));
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
        Map<String, Object> result = new HashMap<>();
        result.put("copyOnWriteArrayList", copyOnWriteArrayList.size());
        result.put("synchronizedList", synchronizedList.size());
        return result;
    }

    // 帮助方法用来填充 List
    private void addAll(List<Integer> list) {
        list.addAll(IntStream.rangeClosed(1, 1000000)
                .boxed()
                .collect(Collectors.toList()));
    }

    // 测试并发读的性能
    @GetMapping("/read")
    public Map<String, Object> testRead() {
        // 创建两个测试对象
        List<Integer> copyOnWriteArrayList = new CopyOnWriteArrayList<>();
        List<Integer> synchronizedList = Collections.synchronizedList(new ArrayList<>());
        // 填充数据
        addAll(copyOnWriteArrayList);
        addAll(synchronizedList);
        StopWatch stopWatch = new StopWatch();
        int loopCount = 1000000;
        int count = copyOnWriteArrayList.size();
        stopWatch.start("Read:copyOnWriteArrayList");
        // 循环 1000000 次并发从 CopyOnWriteArrayList 随机查询元素
        IntStream.rangeClosed(1, loopCount)
                .parallel()
                .forEach(__ -> copyOnWriteArrayList.get(ThreadLocalRandom.current().nextInt(count)));
        stopWatch.stop();

        stopWatch.start("Read:synchronizedList");
        // 循环 1000000 次并发从加锁的 ArrayList 随机查询元素
        IntStream.range(0, loopCount)
                .parallel()
                .forEach(__ -> synchronizedList.get(ThreadLocalRandom.current().nextInt(count)));
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
        Map<String, Object> result = new HashMap<>();
        result.put("copyOnWriteArrayList", copyOnWriteArrayList.size());
        result.put("synchronizedList", synchronizedList.size());
        return result;
    }

    /*
    为何在大量写的场景下，CopyOnWriteArrayList 会这么慢呢？

    答案就在源码中。以 add 方法为例，每次 add 时，都会用 Arrays.copyOf 创建一个新数组，频繁 add 时内存的申请释放消耗会很大：
     */
}
