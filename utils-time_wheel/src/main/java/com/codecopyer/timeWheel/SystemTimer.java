package com.codecopyer.timeWheel;

import java.time.LocalTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class SystemTimer implements Timer, Function<TimerTaskEntry, Void> {

    private ExecutorService taskExecutor;

    private String executeName;

    private Long tickMs;

    private Integer wheelSize;

    /**
     * 起始时间
     */
    private Long startMs;

    private DelayQueue<TimerTaskList> delayQueue = new DelayQueue<>();

    private AtomicInteger taskCounter = new AtomicInteger(0);

    private TimingWheel timingWheel;

    /**
     * Locks used to protect data structures while ticking
     */
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

    private ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public SystemTimer(String executeName) {
        this.executeName = executeName;
        tickMs = 1L;
        wheelSize = 20;
        startMs = Time.getHiresClockMs();
        taskExecutor = new ThreadPoolExecutor(100, 100,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, executeName);
            }
        });
        timingWheel = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, delayQueue);
    }

    /**
     * Add a new task to this executor. It will be executed after the task's delay
     * (beginning from the time of submission)
     *
     * @param timerTask the task to add
     */
    @Override
    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            Long timeDelayTime = timerTask.getDelayMs();
            Long currentSystemTime = Time.getHiresClockMs();
            Long expirationMs = timeDelayTime + currentSystemTime;
            System.out.println("当前时间: "+ LocalTime.now()+" || 启动时间轮,存入任务, 延迟时间(设置的过期时间("+timeDelayTime+")+当前系统时间("+currentSystemTime+")): "+ expirationMs);
            addTimerTaskEntry(new TimerTaskEntry(timerTask, expirationMs));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * SystemTimer 推进方法:
     * Kafka 就利用了空间换时间的思想，通过 DelayQueue，来保存每个槽，通过每个槽的过期时间排序。
     * 这样拥有最早需要执行任务的槽会有优先获取。如果时候未到，那么 delayQueue.poll() 就会阻塞着，
     * 这样就不会有空推进的情况发送
     *
     * @param timeoutMs
     * @return whether or not any tasks were executed
     */
    @Override
    public boolean advanceClock(long timeoutMs) {
        try {
            TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (bucket != null) {
                System.out.println("当前时间: "+ LocalTime.now()+" || poll("+timeoutMs+") => success. 启动时间轮,推进并更新当前指针");
                writeLock.lock();
                try {
                    while (bucket != null) {
                        timingWheel.advanceClock(bucket.getExpiration());  //时间轮根据 bucket的过期时间来更新当前指针
                        System.out.println("当前时间: "+ LocalTime.now()+" || 时间轮指针更新结束");
                        bucket.flush(this);
                        bucket = delayQueue.poll();
                    }
                } finally {
                    writeLock.unlock();
                }
                return true;
            } else {
                System.out.println("当前时间: "+ LocalTime.now()+" || poll("+timeoutMs+") => null");
                return false;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Get the number of tasks pending execution
     *
     * @return the number of tasks
     */
    @Override
    public int size() {
        return taskCounter.get();
    }

    /**
     * ;     * Shutdown the timer service, leaving pending tasks unexecuted
     */
    @Override
    public void shutdown() {
        taskExecutor.shutdown();
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {   //把timerTaskEntry重新add一遍，add的时候会检查任务是否已经到期
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled()) {    //到这里 说明,task 已经过期,那么立即执行.
                System.out.println("当前时间: "+ LocalTime.now()+" || 立即执行任务 (过期时间为: "+ timerTaskEntry.getExpirationMs()+")");
                taskExecutor.submit(timerTaskEntry.getTimerTask());
            }
        } /*else {
            System.out.println("当前时间: "+ LocalTime.now()+" || 尝试执行任务失败: timeTaskEntry的过期时间在在时间轮本层范围内,则放进自己的bucket中 ");
        }*/
    }

    /**
     * Applies this function to the given argument.
     *
     * !!!!!! 把TimerTaskList的任务都取出来重新add一遍，add的时候会检查任务是否已经到期
     *
     * @param timerTaskEntry the function argument
     * @return the function result
     */
    @Override
    public Void apply(TimerTaskEntry timerTaskEntry) {
        System.out.println("当前时间: "+ LocalTime.now()+" || " +
                "取出bucket中的entry尝试重新加入到时间轮中,利用过期判断来触发执行任务"+
                "\n\t\t\t\t\t\t 因为之前poll出来了bucket,然后更新了时间轮的当前指针, 所以后面的[Add Entry]操作会出现 [时间轮降级] (因为时间范围缩小了)\n");
        addTimerTaskEntry(timerTaskEntry);
        return null;
    }
}
