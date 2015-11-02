/**
 * Created by zieng on 10/28/15.
 */
public class CastTestClass
{
    Object key;

    @Override
    public String toString()
    {
        if(key instanceof Integer)
        {
            return "int"+key;
        }
        else
            return "other"+key.toString();
    }
}
