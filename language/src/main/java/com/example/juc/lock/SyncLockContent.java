package com.example.juc.lock;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName SyncLockContent
 * @Description
 * 1 标准访问，先打印短信还是邮件
 * ------------SMS
 * --------------Email
 *
 * 2 停4秒在短信方法内，先打印短信还是邮件
 * ------------SMS
 * --------------Email
 *
 * 3 新增普通的hello方法，是先打短信还是hello
 * ------------SMS
 * ---------------Hello
 *
 * 4 现在有两部手机，先打印短信还是邮件
 * --------------Email
 * ------------SMS sleep4
 *
 * 5 两个静态同步方法，1部手机，先打印短信还是邮件
 * ------------SMS
 * --------------Email
 *
 * 6 两个静态同步方法，2部手机，先打印短信还是邮件
 * ------------SMS
 * --------------Email
 *
 * 7 1个静态同步方法,1个普通同步方法，1部手机，先打印短信还是邮件
 * --------------Email
 * ------------SMS
 *
 * 8 1个静态同步方法,1个普通同步方法，2部手机，先打印短信还是邮件
 * --------------Email
 * ------------SMS
 *
 * 总结：
 * 1 对于普通方法，锁的是当前实例
 * 2 对于静态方法，锁的是当前类的class对象
 * 3 对于同步方法块，锁是synchronized括号里配置的对象
 * @Author
 * @Date 2021/8/27
 */

class Phone {
    public synchronized void sendSMS() throws InterruptedException {
        TimeUnit.SECONDS.sleep(4);
        System.out.println("------------SMS");
    }

    public static synchronized void sendEmail(){
        System.out.println("--------------Email");
    }

    public void getHello(){
        System.out.println("---------------Hello");
    }
}
public class SyncLockContent {
    public static void main(String[] args) throws InterruptedException {
        Phone p1 = new Phone();
        Phone p2 = new Phone();

        new Thread(() -> {
            try {
                p1.sendSMS();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "AA").start();

        Thread.sleep(100);

        new Thread(() -> {
            // p1.sendEmail();
            // p1.getHello();
            p2.sendEmail();
        }, "BB").start();
    }
}