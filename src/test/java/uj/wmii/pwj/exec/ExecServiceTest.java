package uj.wmii.pwj.exec;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class ExecServiceTest {

    @Test
    void testExecute() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.execute(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnable() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.submit(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnableWithResult() throws Exception {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        Object expected = new Object();
        Future<Object> f = s.submit(r, expected);
        doSleep(10);
        assertTrue(r.wasRun);
        assertTrue(f.isDone());
        assertEquals(expected, f.get());
    }

    @Test
    void testScheduleCallable() throws Exception {
        MyExecService s = MyExecService.newInstance();
        StringCallable c = new StringCallable("X", 10);
        Future<String> f = s.submit(c);
        doSleep(20);
        assertTrue(f.isDone());
        assertEquals("X", f.get());
    }

    @Test
    void testShutdown() {
        ExecutorService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        doSleep(10);
        s.shutdown();
        assertThrows(
            RejectedExecutionException.class,
            () -> s.submit(new TestRunnable()));
    }

    @Test
    void testShutdownNow() {
        MyExecService s = MyExecService.newInstance();
        int processors = Runtime.getRuntime().availableProcessors();
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        for (int i = 0; i < processors; i++) {
            s.submit(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                }
                return null;
            });
        }

        s.submit(() -> "Queued");

        java.util.List<Runnable> notExecuted = s.shutdownNow();

        latch.countDown();

        assertFalse(notExecuted.isEmpty(), "Should return tasks that were in queue");
        assertTrue(s.isShutdown());
    }

    @Test
    void testInvokeAll() throws Exception {
        MyExecService s = MyExecService.newInstance();

        java.util.List<Callable<String>> tasks = java.util.Arrays.asList(
            () -> "A",
            () -> "B",
            () -> "C"
        );

        java.util.List<Future<String>> futures = s.invokeAll(tasks);

        assertEquals(3, futures.size());
        assertEquals("A", futures.get(0).get());
        assertEquals("B", futures.get(1).get());
        assertEquals("C", futures.get(2).get());
    }

    @Test
    void testInvokeAllWithTimeout() throws Exception {
        MyExecService s = MyExecService.newInstance();

        java.util.List<Callable<String>> tasks = java.util.Arrays.asList(
            () -> { doSleep(10); return "Fast"; },
            () -> { doSleep(500); return "Slow"; }
        );

        java.util.List<Future<String>> futures = s.invokeAll(
            tasks, 100, java.util.concurrent.TimeUnit.MILLISECONDS
        );

        assertEquals(2, futures.size());

        assertFalse(futures.get(0).isCancelled());
        assertEquals("Fast", futures.get(0).get());

        assertTrue(futures.get(1).isCancelled(), "Slow task should be cancelled");
    }

    @Test
    void testInvokeAny() throws Exception {
        MyExecService s = MyExecService.newInstance();

        java.util.List<Callable<String>> tasks = java.util.Arrays.asList(
            () -> { doSleep(500); return "Slow"; },
            () -> { doSleep(10); return "Fast"; }
        );

        String result = s.invokeAny(tasks);
        assertEquals("Fast", result);
    }

    @Test
    void testInvokeAnyWithTimeout() throws Exception {
        MyExecService s = MyExecService.newInstance();

        java.util.List<Callable<String>> tasks = java.util.Collections.singletonList(
            () -> { doSleep(500); return "Too Slow"; }
        );

        assertThrows(java.util.concurrent.TimeoutException.class, () -> {
            s.invokeAny(tasks, 50, java.util.concurrent.TimeUnit.MILLISECONDS);
        });
    }

    @Test
    void testTaskException() {
        MyExecService s = MyExecService.newInstance();

        Future<Object> f = s.submit(() -> {
            throw new RuntimeException();
        });

        assertThrows(java.util.concurrent.ExecutionException.class, () -> {
            f.get();
        });
    }


    static void doSleep(int milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

class StringCallable implements Callable<String> {

    private final String result;
    private final int milis;

    StringCallable(String result, int milis) {
        this.result = result;
        this.milis = milis;
    }

    @Override
    public String call() throws Exception {
        ExecServiceTest.doSleep(milis);
        return result;
    }
}
class TestRunnable implements Runnable {

    boolean wasRun;
    @Override
    public void run() {
        wasRun = true;
    }
}
