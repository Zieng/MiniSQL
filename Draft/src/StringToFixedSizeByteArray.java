/**
 * Created by zieng on 10/16/15.
 */
public class StringToFixedSizeByteArray
{
    public static void main(String [] args)
    {
        String str="hello";
        byte [] b = str.getBytes();

        System.out.println(str.length());
        System.out.println(b.length);
    }
}
