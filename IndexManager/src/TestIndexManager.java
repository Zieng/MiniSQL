import java.util.ArrayList;

/**
 * Created by zieng on 10/16/15.
 */
public class TestIndexManager
{
    public static void main(String [] args)
    {
        IndexContext ic = new IndexContext();
        ic.filename = "namestu.index";
        ic.rootBlock = 2;
        ic.existBlock = new ArrayList<Integer>(2);
        ic.keyType = "char";
        ic.keyLength = 1000;


        return;
    }
}
