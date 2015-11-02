import java.util.Comparator;

/**
 * Created by zieng on 10/27/15.
 */
public class BplusLeafNode_float
{
    float key;
    int blockNum;
    int recordNum;

    public static Comparator<BplusLeafNode_float> keyComp = new Comparator<BplusLeafNode_float>()
    {
        public int compare(BplusLeafNode_float o1, BplusLeafNode_float o2)
        {
            Float k1=o1.key;
            Float k2=o2.key;
            return k1.compareTo(k2);
        }
    };
}
