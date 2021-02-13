package com.codecopyer.timeWheel;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class SystemTimerTest {

    static int addedTime = 100;  //外层系统每次推进时间(ms)



    public static void main(String[] args) {
        SystemTimer systemTimer = new SystemTimer("timer");

        //System.out.println("启动 时间: "+ System.currentTimeMillis());
        //DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        //System.out.println(timeFormatter.format(nowTime));

        System.out.println("当前时间: "+ LocalTime.now());


//        for (int i = 0; i < 100; i++) {
//            systemTimer.add(new DelayedOperation(500+i*1000));
//        }

        systemTimer.add(new DelayedOperation(200));
        systemTimer.add(new DelayedOperation(300));
//        systemTimer.add(new DelayedOperation(5000));

        //System.out.println(System.nanoTime());
        boolean flag = true;
        while (flag) {
            boolean b = systemTimer.advanceClock(addedTime);
            if(!b) {
                System.out.println("当前时间: "+LocalTime.now()+" || 外层系统时间 推进: "+ addedTime +"ms");
            }
            //flag = b;
        }
    }
}
