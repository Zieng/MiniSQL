import java.util.Comparator;

/**
 * Created by zieng on 10/27/15.
 */
public class BplusInnerNode_char
{
    String key;
    int blockNum;

    public static Comparator<BplusLeafNode_char> keyComp = new Comparator<BplusLeafNode_char>()
    {
        public int compare(BplusLeafNode_char o1, BplusLeafNode_char o2)
        {
            return o1.key.compareTo(o2.key);
        }
    };
}
