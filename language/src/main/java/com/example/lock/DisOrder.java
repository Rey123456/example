package com.example.lock;

/**
 * @ClassName DisOrder
 * @Description TODO
 * @Author rey
 * @Date 2021/8/13
 */
public class DisOrder {
    private static int x=0,y=0;
    private static int a=0,b=0;

    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        for(;;){
            i++;
            x = 0; y = 0;
            a = 0; b = 0;
            Thread one = new Thread(new Runnable(){
                @Override
                public void run() {
                    a = 1;
                    x = b;
                }
            });

            Thread other = new Thread(new Runnable(){
                @Override
                public void run() {
                    b = 1;
                    y = a;
                }
            });
            one.start();
            other.start();
            one.join();
            other.join();
            String result = i + "次" + x + "," + y;
            if(x==0 && y==0){
                System.out.println(result);
                break;
            }else{
                System.out.println(result);
            }
        }
    }
}