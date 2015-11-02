import java.util.Comparator;

/**
 * Created by zieng on 10/28/15.
 */
public class Comp
{
    public Object key;

    public int compareTo(Comp other)
    {
        if(key instanceof Integer)
        {
            return ((Integer) key).compareTo((Integer)other.key);
        }
        else if(key instanceof Float)
        {
            return ((Float) key).compareTo( (Float) other.key);
        }
        return -1;
    }

    public static Comparator<Comp> keyComp = new Comparator<Comp>()
    {
        public int compare(Comp o1, Comp o2)
        {
           return o1.compareTo(o2);
        }
    };

    public String toStirng()
    {
        String str;
        if(key instanceof Integer)
            str= ( (Integer)key).toString();
        else if( key instanceof  Float)
            str = key.toString();
        else
            str="fail";

        return str;
    }
}
