import java.util.Comparator;

/**
 * Created by zieng on 10/27/15.
 */
public class BplusInnerNode_int
{
    int key;
    int blockNum;

    public static Comparator<BplusInnerNode_int> keyComp = new Comparator<BplusInnerNode_int>()
    {
        public int compare(BplusInnerNode_int o1, BplusInnerNode_int o2)
        {
            Integer k1=o1.key;
            Integer k2=o2.key;
            return k1.compareTo(k2);
        }
    };

}
