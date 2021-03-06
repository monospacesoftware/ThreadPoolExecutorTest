package com.monospacesoftware;

import org.testng.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

public class ThreadPoolExecutorTest {

    private AtomicInteger threadCounter = new AtomicInteger(0);
    private ThreadPoolExecutor executor;

    @BeforeMethod
    private void reset() {
        threadCounter.set(0);
    }

    private void runTest(int core, int max, int tasks, BlockingQueue<Runnable> workQueue, boolean allowCoreThreadTimeOut) {
        executor = new ThreadPoolExecutor(core, max,
                2, TimeUnit.SECONDS,
                workQueue,
                r -> new Thread(r, "ThreadPoolExecutorTestThread-" + threadCounter.getAndIncrement()));
        executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);

        IntStream.range(0, tasks)
                .mapToObj(i -> CompletableFuture.runAsync(() -> System.out.printf("%s: %s\n", Thread.currentThread().getName(), i), executor))
                .collect(Collectors.toList())
                .stream()
                .forEach(CompletableFuture::join);
    }

    @AfterMethod
    private void shutdown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(executor.isShutdown()).isTrue();
    }

    // core=max=10, queue is unbounded
    // 5 task: no more than 5 threads should be created
    @Test
    public void test__10_10_5_UNBOUNDED() throws Exception {
        runTest(10, 10, 5, new LinkedBlockingQueue<>(), false);

        assertThat(executor.getPoolSize()).isEqualTo(5);
    }

    // core=max=10, queue is unbounded (Executors.newFixedThreadPool)
    // 50 task: no more than max threads should be created
    // This is the typical thread pool except the pool never shrinks because core=max,
    // so for intermittently running tasks, it's a waste of memory
    @Test
    public void test__10_10_50_UNBOUNDED() throws Exception {
        runTest(10, 10, 50, new LinkedBlockingQueue<>(), false);

        assertThat(executor.getPoolSize()).isEqualTo(10);

        TimeUnit.SECONDS.sleep(3);
        assertThat(executor.getPoolSize()).isEqualTo(10);
    }

    // core=max=10, queue is unbounded (Executors.newFixedThreadPool)
    // 50 task: no more than max threads should be created
    // This is a polite thread pool: grow to max size as needed, adding to an unbounded queue...
    // but all threads go away after keepAliveTimeout
    @Test
    public void test__10_10_50_UNBOUNDED_CORE_TIMEOUT() throws Exception {
        runTest(10, 10, 50, new LinkedBlockingQueue<>(), true);
        executor.allowCoreThreadTimeOut(true);

        assertThat(executor.getPoolSize()).isEqualTo(10);

        TimeUnit.SECONDS.sleep(3);
        assertThat(executor.getPoolSize()).isEqualTo(0);
    }

    // core=1, max=10, queue is unbounded
    // 50 tasks: pool size will not grow beyond core unless the queue reaches capacity, which won't happen since it's unbounded
    // So pool size gets capped at core size!  This is weird the unexpected case we've run into that can cause problems.
    @Test
    public void test__1_10_50_UNBOUNDED() throws Exception {
        runTest(1, 10, 50, new LinkedBlockingQueue<>(), false);

        assertThat(executor.getPoolSize()).isEqualTo(1);

        TimeUnit.SECONDS.sleep(3);
        assertThat(executor.getPoolSize()).isEqualTo(1);
    }

    // core=1, max=10, queue capacity=5
    // 50 tasks: once the queue fills to capacity, ThreadPoolExecutor will increase threads beyond core up to max,
    // but once we are at max threads AND the queue is full, BlockQueue will reject the additional task and throw a RejectedExecutionException
    @Test
    public void test__1_10_50_BOUNDED_5() throws Exception {
        try {
            runTest(1, 10, 50, new ArrayBlockingQueue<>(5), false);
            fail("Expected RejectedExecutionException");
        } catch (RejectedExecutionException e) {
        }

        assertThat(executor.getPoolSize()).isEqualTo(10);
    }

    // core=1, max=10, queue capacity=10
    // 20 tasks: this is a race... tasks are added to the queue until we reach capacity, then the thread pool size
    // starts to increase to max.  if we reach capacity and we're at max threads, we'll get a RejectedExecutionException
    // but the threads are also actively pulling from the queue driving down the size.
    // Pool size should be > core but <= max
    // core=1 so pool should shrink to 1 after keepAliveTime
    @Test
    public void test__1_10_20_BOUNDED_10() throws Exception {
        runTest(1, 10, 20, new ArrayBlockingQueue<>(10), false);

        assertThat(executor.getPoolSize()).isBetween(1, 10);

        TimeUnit.SECONDS.sleep(3);
        assertThat(executor.getPoolSize()).isEqualTo(1);
    }

    // core=0, max=Integer.MAX_VALUE, synchronous queue (Executors.newCachedThreadPool)
    // 50 tasks: a synchronous queue can only accept 1 item, causing the pool size grows until the numbers of
    // thread can handle the incoming load.  This is dangerous if threads can not keep up with the load, which would cause unbounded thread growth.
    // core=0 so all threads go away after keepAliveTime
    @Test
    public void test__0_MAX_VALUE_50_SYNCHRONOUS() throws Exception {
        runTest(0, Integer.MAX_VALUE, 20, new SynchronousQueue<>(), false);

        assertThat(executor.getPoolSize()).isGreaterThan(1);

        TimeUnit.SECONDS.sleep(3);
        assertThat(executor.getPoolSize()).isEqualTo(0);

    }

}
