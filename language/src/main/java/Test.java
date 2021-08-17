import org.openjdk.jol.info.ClassLayout;

/**
 * @ClassName Test
 * @Description TODO
 * @Author rey
 * @Date 2021/8/12
 */
public class Test {
    public static void main(String[] args) {
        Object o = new Object();
        System.out.println(ClassLayout.parseInstance(o).toPrintable());

        synchronized (o) {
            System.out.println(ClassLayout.parseInstance(o).toPrintable());
        }
    }
}