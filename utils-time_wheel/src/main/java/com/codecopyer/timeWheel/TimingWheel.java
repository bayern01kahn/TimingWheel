package com.codecopyer.timeWheel;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.LocalTime;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

@NotThreadSafe
public class TimingWheel {

    /**
     * 每一格时间(基本时间跨度)
     */
    private Long tickMs;
    /**
     * 格子数(时间格个数)
     */
    private Integer wheelSize;

    /**
     * 整个时间轮的总体时间跨度 (tickMs × wheelSize)
     */
    private Long interval;

    /**
     * 起始时间
     */
    private Long startMs;

    private AtomicInteger taskCounter;

    /**
     * DelayQueue专门负责时间推进的任务
     */
    private DelayQueue<TimerTaskList> queue;

    /**
     * 表盘指针
     * 用来表示时间轮当前所处的时间，currentTime是tickMs的整数倍。
     * currentTime可以将整个时间轮划分为到期部分和未到期部分，currentTime当前指向的时间格也属于到期部分，表示刚好到期，
     * 需要处理此时间格所对应的TimerTaskList的所有任务
     */
    private Long currentTime;

    /**
     * 上层时间轮
     */
    private volatile TimingWheel overflowWheel;

    private TimerTaskList[] buckets;

    public TimingWheel(Long tickMs, Integer wheelSize, Long startMs, AtomicInteger taskCounter, DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.interval = tickMs * wheelSize;
        this.currentTime = startMs - (startMs % tickMs);
        this.buckets = new TimerTaskList[wheelSize];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new TimerTaskList(taskCounter);
        }
        System.out.println("当前时间: "+LocalTime.now()+" || 上层时间轮("+interval+")=>创建: startMs("+startMs+") currentTime("+currentTime+")");
    }

    /**
     * 添加任务
     *
     * Kafka中的TimingWheel专门用来执行插入和删除TimerTaskEntry的操作
     */
    public boolean add(TimerTaskEntry timerTaskEntry) {
        long expiration = timerTaskEntry.getExpirationMs();

        if (timerTaskEntry.cancelled()) {
            // Cancelled
            return false;
        } else if (expiration < currentTime + tickMs) {  //如果已经到期，返回false
            // Already expired
            System.out.println("当前时间: "+ LocalTime.now()+" || [Add Entry] 过程中发现当前任务到期!! ,过期时间为: "+ timerTaskEntry.getExpirationMs());
            return false;
        } else if (expiration < currentTime + interval) {  //如果在本层范围内

            // Put in its own bucket
            long virtualId = expiration / tickMs;
            int slot = (int) virtualId % wheelSize;  //计算槽位
            TimerTaskList bucket = buckets[slot];
            bucket.add(timerTaskEntry);  // 添加到槽内的双向链表中

            System.out.println("当前时间: "+LocalTime.now()+" || " +
                    "时间轮处理=> [Add Entry] 推进时间("+expiration+") " +
                    "< 时间轮["+interval+"]范围("+(currentTime+interval)+"):[当前时间("+currentTime+") + 本层最大时间interval("+interval+")] " +
                    "[放入] bucket["+slot+"]中 ");

            // Set the bucket expiration time
            System.out.println("当前时间: "+ LocalTime.now()+" || 更新bucket["+slot+"]的过期时间: " + virtualId * tickMs);
            if (bucket.setExpiration(virtualId * tickMs)) {   //更新槽过期时间
                // The bucket needs to be enqueued because it was an expired bucket
                // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
                // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
                // will pass in the same value and hence return false, thus the bucket with the same expiration will not
                // be enqueued multiple times.
                queue.offer(bucket);   // 将槽加入到delayQueue,通过delayQueue来推进时间
                System.out.println("当前时间: "+ LocalTime.now()+" || 将该bucket["+slot+"]存入delayQueue");
            }
            return true;
        } else {   // 如果超过本层能表示的延迟时间则将任务添加到上层，这里可以看到上层是按需创建的
            System.out.println("当前时间: "+LocalTime.now()+" || " +
                    "时间轮处理=> [Add Entry] 推进时间("+expiration+") " +
                    "> 时间轮["+interval+"]范围("+(currentTime+interval)+"):[当前时间("+currentTime+") + 本层最大时间interval("+interval+")]" +
                    ", 移交 [上层时间轮] 处理");

            // Out of the interval. Put it into the parent timer
            if (overflowWheel == null) {
                addOverflowWheel();
            }
            return overflowWheel.add(timerTaskEntry);
        }
    }

    /**
     * 时间轮 的 推进
     */
    public void advanceClock(Long timeMs) {
        if (timeMs >= currentTime + tickMs) {              //推进时间 需要 >= 当前时间+ 一格的时间

            /**
             * 推进时间轮的指针
             *
             * 除了第一层时间轮，其余高层时间轮的起始时间(startMs)都设置为创建此层时间轮时前面第一轮的currentTime。
             * 每一层的currentTime都必须是tickMs的整数倍，如果不满足则会将currentTime修剪为tickMs的整数倍，以此与时间轮中的时间格的到期时间范围对应起来。
             * 修剪方法为：currentTime = startMs - (startMs % tickMs)。currentTime会随着时间推移而推进，但是不会改变为tickMs的整数倍的既定事实。
             * 某时刻的时间为timeMs，那此时的currentTime = timeMs - (timeMs % tickMs)，时间每推进一次，每层级时间轮的currentTime都会依据此公式推进
             */
            currentTime = timeMs - (timeMs % tickMs);      //修剪 currentTime 为 tickMs的整数倍

            System.out.println("当前时间: "+LocalTime.now()+" || " +
//                    " Thread: "+Thread.currentThread()+
                    "时间轮["+interval+"] " +
//                    " 起始时间: "+ startMs+
                    "推进时间: "+timeMs+ "  "+
//                    " (差值: "+(timeMs-startMs)+")"+
//                    "\n\t\t\t\t\t\t 需修剪掉的时间: "+timeMs % tickMs+
                    "更新本层指针: "+ currentTime);

            if (overflowWheel != null) {                   //如果存在上级时间轮 则 递归调用 使用当前时间推进上级时间轮
                overflowWheel.advanceClock(currentTime);
            }
        }
    }


    /**
     * 增加溢出时间轮
     */
    private void addOverflowWheel() {
        synchronized (this) {
            if (overflowWheel == null) {

                overflowWheel = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue);
            }
        }
    }
}
