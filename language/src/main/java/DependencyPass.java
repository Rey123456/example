/**
 * @ClassName DependencyPass
 * @Description 学习视频中案例，依赖关系传递的三种方式和应用案例
 * @Author rey
 * @Date 2021/8/12
 */
public class DependencyPass {
    public static void main(String[] args) {
        ChangHong changHong = new ChangHong();

        //通过接口传递实现依赖
        OpenAndClose openAndClose = new OpenAndClose();
        openAndClose.open(changHong);

        //通过构造器进行依赖传递
        // OpenAndClose openAndClose = new OpenAndClose(changHong);
        // openAndClose.open();

        //通过 setter 方法进行依赖传递
        // OpenAndClose openAndClose = new OpenAndClose();
        // openAndClose.setTv(changHong);
        // openAndClose.open();

    }
}

interface ITV { // ITV 接口
    public void play();
}
class ChangHong implements ITV {
    public void play() {
        System.out.println("长虹电视机，打开");
    }
}
/**方式 1: 通过接口传递实现依赖*/
interface IOpenAndClose{
    public void open(ITV tv);
}
class OpenAndClose implements IOpenAndClose {
    public void open(ITV tv) {
        tv.play();
    }
}

/**方式 2: 通过构造方法依赖传递*/
// interface IOpenAndClose{
//     public void open();
// }
// class OpenAndClose implements IOpenAndClose{
//     public ITV tv; //成员
//     public OpenAndClose(ITV tv){ //构造器
//         this.tv = tv;
//     }
//     public void open(){
//         this.tv.play();
//     }
// }

/** 方式 3 , 通过 setter 方法传递**/
// interface IOpenAndClose {
//     public void open(); // 抽象方法
//     public void setTv(ITV tv);
// }
// class OpenAndClose implements IOpenAndClose {
//     private ITV tv;
//     public void setTv(ITV tv) {
//         this.tv = tv;
//     }
//     public void open() {
//         this.tv.play();
//     }
// }
