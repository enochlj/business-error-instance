package com.yohlj.java.bei.concurrent;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/threadlocal")
public class ThreadLocalDemo {

    private final ThreadLocal<Integer> currentUser = ThreadLocal.withInitial(() -> null);

    @GetMapping("/wrong")
    public Map<String,Object> wrong(@RequestParam("userId") Integer userId) {
        // 设置用户信息之前先查询一次 ThreadLocal 中的用户信息
        String before = Thread.currentThread().getName() + ":" + currentUser.get();
        // 设置用户信息到 ThreadLocal
        currentUser.set(userId);
        // 设置用户信息之后再查询一次 ThreadLocal 中的用户信息
        String after = Thread.currentThread().getName() + ":" + currentUser.get();
        // 汇总输出两次查询结果
        Map<String,Object> result = new HashMap<>();
        result.put("before", before);
        result.put("after", after);
        return result;
    }

    /*
    程序运行在 Tomcat 中，执行程序的线程是 Tomcat 的工作线程，而 Tomcat 的工作线程是基于线程池的。

    顾名思义，线程池会重用固定的几个线程，一旦线程重用，那么很可能首次从 ThreadLocal 获取的值是之前其他用户的请求遗留的值。
    这时，ThreadLocal 中的用户信息就是其他用户的信息。

    使用类似 ThreadLocal 工具来存放一些数据时，需要特别注意在代码运行完后，显式地去清空设置的数据。
     */

    @GetMapping("right")
    public Map<String, Object> right(@RequestParam("userId") Integer userId) {
        String before = Thread.currentThread().getName() + ":" + currentUser.get();
        currentUser.set(userId);
        try {
            String after = Thread.currentThread().getName() + ":" + currentUser.get();
            Map<String, Object> result = new HashMap<>();
            result.put("before", before);
            result.put("after", after);
            return result;
        } finally {
            // 在 finally 代码块中删除 ThreadLocal 中的数据，确保数据不串
            currentUser.remove();
        }
    }

}
