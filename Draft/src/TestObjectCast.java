import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by zieng on 10/28/15.
 */
public class TestObjectCast
{
    public static void main(String [] args)
    {
//        CastTestClass ctc = new CastTestClass();
//
//        Integer i = new Integer(2);
//        ctc.key=i;
//        System.out.println(ctc.toString());
//
//        Float f = new Float(2.0);
//        ctc.key = f;
//
//        System.out.println(ctc.toString());

//        Comp c1 = new Comp();
//        c1.key = new Integer(100);
//        System.out.println(c1.key);
//        Comp c2 = new Comp();
//        c2.key = new Integer(200);
//        System.out.println(c2.key);
//
//        System.out.println(c1.compareTo(c2));

        ArrayList<Comp> cl = new ArrayList<Comp>();

        for(int i=10;i>=0;i--)
        {
            Comp c = new Comp();
            c.key = new Integer(i);
            cl.add(c);
        }

        Collections.sort(cl,Comp.keyComp);

        for(int i=0;i<10;i++)
        {
            System.out.println(cl.get(i).toStirng()+"");
        }

        ArrayList<Comp> cl2 = new ArrayList<Comp>();

        for(int i=10;i>=0;i--)
        {
            Comp c = new Comp();
            c.key = new Float(i);
            cl2.add(c);
        }
        Collections.sort(cl2,Comp.keyComp);
        for(int i=0;i<10;i++)
        {
            System.out.println(cl2.get(i).toStirng()+"");
        }
    }

}
