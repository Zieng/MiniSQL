import java.util.ArrayList;

/**
 * Created by zieng on 10/24/15.
 */
public class BplusBlock
{
    int currentBlock;
    int parentBlock;  // the parent of root block is -1
    int type;      // type='l'-->leaf block or type='i'--->non-leaf block
    ArrayList nodeList;
    int nextBlockNum;
}
