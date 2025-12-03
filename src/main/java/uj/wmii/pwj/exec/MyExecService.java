package uj.wmii.pwj.exec;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;
import java.util.ArrayList;

import java.util.function.Consumer;

public class MyExecService implements ExecutorService {
    final private int numThreads;
    final private List<Thread> threads;
    final private BlockingDeque<Runnable> tasks;

    private volatile boolean isShutdown;

    private class MyThread implements Runnable
    {   
        @Override
        public void run()
        {
            while(true)
            {   
                if(isShutdown && tasks.isEmpty())
                {
                    return;
                }

                try
                {
                    Runnable task = tasks.pollFirst(1, TimeUnit.SECONDS);

                    if(task != null)
                    {
                        task.run();
                    }
                }
                catch(InterruptedException e)
                {   
                    System.out.println(Thread.currentThread().getName() + ": " + e.getMessage());
                    break;
                }
            }
        }
    }

    private MyExecService(Integer numThreads)
    {   
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if(numThreads == null || (int)numThreads > availableProcessors)
        {
            this.numThreads = availableProcessors;
        }
        else if(numThreads < 1)
        {
            this.numThreads = 1;
        }
        else
        {
            this.numThreads = (int)numThreads;
        }

        threads = new ArrayList<Thread>(this.numThreads);

        tasks = new LinkedBlockingDeque<>();
        isShutdown = false;

        for(int i = 0; i < this.numThreads; i++)
        {   
            Thread newThread = new Thread(new MyThread());
            threads.add(newThread);
            newThread.start();
        }

    };

    static MyExecService newInstance() 
    {
        return new MyExecService(null);
    }

    @Override
    public void shutdown() 
    {
        synchronized(this)
        {
            isShutdown = true;
        }
    }

    @Override
    public List<Runnable> shutdownNow() 
    {   
        synchronized(this)
        {
            if (isShutdown)
            {   
                return List.of();
            }
            isShutdown = true;
            
            threads.forEach(t -> t.interrupt());
        }

        ArrayList<Runnable> notCompleted = new ArrayList<>();
        tasks.drainTo(notCompleted);

        return notCompleted;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public boolean isTerminated() 
    {
        if(!isShutdown) return false;

        for(Thread t : threads)
        {
            if(t.isAlive())
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException 
    {

        long timeoutNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + timeoutNanos;

        for(Thread t : threads)
        {
            while(t.isAlive())
            {
                long remaining = deadline - System.nanoTime();
                if(remaining <= 0) return false;
                
                long milis = TimeUnit.NANOSECONDS.toMillis(remaining);
                int nanos = (int)(remaining - TimeUnit.MILLISECONDS.toNanos(milis));

                t.join(milis, nanos);           
            }
        }

        return true;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) 
    {   
        if(task == null)
        {
            throw new NullPointerException("task cannot be null");
        }

        FutureTask<T> future = new FutureTask<>(task);
        
        synchronized(this)
        {       
            if(isShutdown)
            {
                throw new RejectedExecutionException("Shutdown");
            }

            tasks.addLast(future);
            return future;
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) 
    {   
        if(task == null)
        {
            throw new NullPointerException("task cannot be null");
        }

        FutureTask<T> future = new FutureTask<>(task, result);

        synchronized(this)
        {
            if(isShutdown)
            {
                throw new RejectedExecutionException("Shutdown");
            }
            
            tasks.addLast(future);
        }

        return future;    
    }

    @Override
    public Future<?> submit(Runnable task) 
    {
        if(task == null)
        {
            throw new NullPointerException("task cannot be null");
        }

        FutureTask<?> future = new FutureTask<>(task, null);

        synchronized(this)
        {
            if(isShutdown)
            {
                throw new RejectedExecutionException("Shutdown");
            }

            tasks.addLast(future);            
        }
        
        return future;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException 
    {
        if(isShutdown)
        {
            throw new RejectedExecutionException("Shutdown");
        }

        if(tasks == null)
        {
            throw new NullPointerException("Tasks cannot be null");
        }

        List<Future<T>> futures = new ArrayList<>();
        boolean done = false;

        try
        {
            for(Callable<T> task : tasks)
            {   
                futures.add(submit(task));
            }

            for(Future<T> future : futures)
            {   
                if(!future.isDone())
                {
                    
                    try
                    {
                        future.get();
                    }
                    catch(ExecutionException | CancellationException e) {}
                }
            }

            done = true;
            return futures;
        }
        finally
        {
            if(!done)
            {
                for(Future<T> f2 : futures)
                {
                    if(!f2.isDone())
                    {
                        f2.cancel(true);
                    }
                }
            }
        }
        
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException 
    {
        if(isShutdown)
        {
            throw new RejectedExecutionException("Shutdown");
        }

        if(tasks == null)
        {
            throw new NullPointerException("Tasks cannot be null");
        }
        
        long timeoutNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + timeoutNanos;

        List<Future<T>> futures = new ArrayList<>();
        boolean done = false;

        try
        {   
            long remaining;
            boolean firstTimedOut = false;

            for(Callable<T> task : tasks)
            {   
                remaining = deadline - System.nanoTime();

                if(remaining > 0)
                {
                    futures.add(submit(task));                
                }
                else
                {   
                    if(!firstTimedOut)
                    {
                        for(Future<T> activeTask : futures)
                        {
                            activeTask.cancel(true);
                        }

                        firstTimedOut = true;
                        done = true;
                    }
                    
                    FutureTask<T> f = new FutureTask<>(task);
                    f.cancel(true);
                    futures.add(f);
                }
            }
            
            for(Future<T> future : futures)
            {   
                remaining = deadline - System.nanoTime();
                
                if(remaining <= 0)
                {
                    return futures;
                }

                if(!future.isDone())
                {                    
                    try
                    {
                        future.get(remaining, TimeUnit.NANOSECONDS);
                    }
                    catch(TimeoutException e)
                    {
                        return futures;
                    }
                    catch(ExecutionException | CancellationException e) 
                    {}
                }
            }

            done = true;
            return futures;
        }
        finally
        {
            if(!done)
            {
                for(Future<T> f2 : futures)
                {
                    if(!f2.isDone())
                    {
                        f2.cancel(true);
                    }
                }
            }
        }        
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException 
    {
        try 
        {
            return invokeAny(tasks, 0, null); 
        } 
        catch (TimeoutException e) 
        {
            throw new ExecutionException(e);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException 
    {
        if(isShutdown)
        {
            throw new RejectedExecutionException("Shutdown");
        }

        if(tasks == null)
        {
            throw new NullPointerException("Tasks cannot be null");
        }

        if(tasks.size() == 0)
        {
            throw new IllegalArgumentException("Tasks cannot be empty");
        }
        
        boolean timed = true;

        if(timeout == 0 && unit == null)
        {
            timed = false;
        }
        
        final Object mutex = new Object();
        final int totalTasks = tasks.size();

        AtomicInteger failureCounter = new AtomicInteger(0);
        AtomicReference<T> successResult = new AtomicReference<>(null);

        List<Future<?>> futures = new ArrayList<>();
        
        Consumer<List<Future<?>>> cleaner = (toClean) -> 
        {
            for(Future<?> activeTask : toClean)
            {
                if(!activeTask.isDone())
                {
                    activeTask.cancel(true);
                }
            }
        };
        

        try
        {
            for(Callable<T> task : tasks)
            {
                Runnable wrapper = () ->
                {
                    try
                    {
                        if (successResult.get() != null) return;
                        
                        T result = task.call();
                        
                        synchronized(mutex)
                        {
                            if (successResult.compareAndSet(null, result))
                            {
                                mutex.notify();
                            }
                        }

                    }
                    catch(Exception e)
                    {
                        int currFails = failureCounter.incrementAndGet();

                        synchronized(mutex)
                        {
                            if(currFails == totalTasks)
                            {
                                mutex.notify();
                            }    
                        }
                    }
                };

                futures.add(submit(wrapper));
            }
        }
        catch(Exception e)
        {
            cleaner.accept(futures);
            throw e;
        }

        long deadline = 0;
        
        if(timed)
        {
            deadline = System.nanoTime() + unit.toNanos(timeout);
        }

        synchronized(mutex)
        {   
            try
            {
                while(successResult.get() == null && failureCounter.get() < tasks.size())
                {
                    if(timed)
                    {
                        long remainingNanos = deadline - System.nanoTime();
                        
                        if (remainingNanos <= 0)
                        {
                            throw new TimeoutException(); 
                        }

                        long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
                        int nanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(millis));

                        mutex.wait(millis, nanos);    
                    }
                    else
                    {
                        mutex.wait();
                    }
                }    
            }
            finally
            {
                cleaner.accept(futures);
            }
        }

        if (successResult.get() != null)
        {   
            return successResult.get();
        }

        throw new ExecutionException(new Exception(""));
    }

    @Override
    public void execute(Runnable command) 
    {   
        if(command == null)
        {
            throw new NullPointerException("command to execute cannot be null");
        }

        synchronized(this)
        {
            if(isShutdown)
            {
                throw new RejectedExecutionException("Shutdown");
            }

            tasks.addLast(command);            
        }    
    }
}
