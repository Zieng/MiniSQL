import java.util.Comparator;

/**
 * Created by zieng on 10/24/15.
 */
public class BplusInnerNode_float
{
    float key;
    int blockNum;

    public static Comparator<BplusInnerNode_float> keyComp = new Comparator<BplusInnerNode_float>()
    {
        public int compare(BplusInnerNode_float o1, BplusInnerNode_float o2)
        {
            Float k1=o1.key;
            Float k2=o2.key;
            return k1.compareTo(k2);
        }
    };
}
