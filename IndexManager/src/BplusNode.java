import java.util.Comparator;

/**
 * Created by zieng on 10/28/15.
 */
public class BplusNode
{
    public Object key;
    public int blockNum;
    public int recordNum;

    public BplusNode clone()
    {
        BplusNode c = new BplusNode();

        if( key instanceof Integer)
            c.key = new Integer((Integer)key) ;
        else if(key instanceof Float)
            c.key = new Float((Float)key);
        else if(key instanceof String)
            c.key = new String( (String) key);

        c.blockNum = blockNum;
        c.recordNum = recordNum;

        return c;
    }

    public int compareTo(BplusNode otherNode)
    {
        if(otherNode == null)
            System.out.println("otherNode is null");

        if( key instanceof Integer)
        {
            return ((Integer) key).compareTo( (Integer) otherNode.key);
        }
        else if(key instanceof Float)
        {
            return ((Float) key).compareTo( (Float)otherNode.key);
        }
        else if(key instanceof String)
        {
            return ((String) key).compareTo( (String) otherNode.key);
        }
        else
        {
            System.out.println("\tBplusNode::compareTo-->undefined key type!");
            return 0;
        }
    }

    public static Comparator<BplusNode> keyComp = new Comparator<BplusNode>()
    {
        public int compare(BplusNode o1, BplusNode o2)
        {
            return o1.compareTo(o2);
        }
    };

    public String toString()
    {
        String str="{-!key is null!-}";
        if(key!=null)
        {
            str = "key="+key.toString()+",block="+blockNum;
            if(recordNum >= 0)
                str+=",record="+recordNum;
        }
        return str;
    }


}
