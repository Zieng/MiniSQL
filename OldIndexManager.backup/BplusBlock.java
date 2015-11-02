import java.util.ArrayList;

/**
 * Created by zieng on 10/24/15.
 */
public class BplusBlock
{
    enum TYPE{INNER,LEAF}

    int currentBlock;
    int parentBlock;  // the parent of root block is -1
    TYPE type;      // type='l'-->leaf block or type='i'--->non-leaf block
    int nodeNum;   // how many BplusInnerNode or BplusLeafNode in a block
    ArrayList nodeList;
    int nextBlockNum;
    boolean endFlag;    // endFlag='$'--->it's the last leaf node(block) ; or endFlag='>'---> has right siblings
                    // inner block don't need the endFlag
  }
