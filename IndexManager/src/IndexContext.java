import java.util.ArrayList;
import java.util.Stack;

/**
 * Created by zieng on 10/27/15.
 */
public class IndexContext
{
    int rootBlock;
    ArrayList<Integer> existBlock;
    String filename;

    ArrayList<Integer> resultBlockSet;
    ArrayList<Integer> resultRecordSet;

//    ArrayList<BplusNode> toDeleteNodes;   // for delete

    BplusNode keyNode;
    int blockContainsKeyNode;

    String keyType;
    int keyLength;

    int maxInnerNode;
    int maxLeafNode;

    Stack<Integer> searchPath;

//    public IndexContext(String keyValue,String keyType,int keyLength,int keyBlockNum,int keyRecordNum,)
}
