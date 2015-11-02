/**
 * Created by zieng on 10/12/15.
 */
public class TestValuePassing
{
    public static void main(String [] args)
    {
        String [] str = new String[2];
        setStringArray(str);
        for(int i=0;i<2;i++)
        {
            System.out.println(str[i]);
        }
    }
    public static void setStringArray(String [] ss)
    {
        String [] temp = new String[2];
        temp[0]="hello";
        temp[1]="world";
//        ss=temp;
        ss[0]="hello";
        ss[1]="world";
    }
}
