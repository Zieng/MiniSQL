import java.util.Comparator;
import java.util.Objects;

/**
 * Created by zieng on 10/27/15.
 */
public class BplusLeafNode_int
{
    int key;
    int blockNum;
    int recordNum;

    public static Comparator<BplusLeafNode_int> keyComp = new Comparator<BplusLeafNode_int>()
    {
        public int compare(BplusLeafNode_int o1, BplusLeafNode_int o2)
        {
            Integer k1=o1.key;
            Integer k2=o2.key;
            return k1.compareTo(k2);
        }
    };
}
