package com.codecopyer.timeWheel;

import java.time.LocalTime;

public class DelayedOperation extends TimerTask {

    public DelayedOperation(long delayMs) {
        super.delayMs = delayMs;
    }

    @Override
    public void run() {
        LocalTime nowTime = LocalTime.now();
        System.out.println("\n\n\n\n当前时间: "+ nowTime+ " || 定时任务: (预设过期时间: "+delayMs+")开始执行 !!!!!! \n\n\n\n");
        //System.out.println("定时任务 执行开始"+ System.currentTimeMillis());
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
