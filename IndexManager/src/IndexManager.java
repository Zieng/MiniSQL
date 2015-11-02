import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.SynchronousQueue;


/**
 * Created by zieng on 10/16/15.
 */

/*
the raw data block structure is:
struct block
{
    int type;         // type = 0 --menas inner;   type =1 ----means leaf
    int nodeList.size();
    node [] nodeList;
    int nextBlock;  // -1 means null

}

the raw node data structure is:
    key+block       // inner node
or
    key+block+record     // leaf node

 */
public class IndexManager
{
    //????
    static IndexContext IC;

    private static BplusBlock BPB;

    public static void set_index_context(IndexContext index_context)  // index_context at least need:filename,keyLength,keyType,rootBlock,existBlock
    {
        IC=index_context;

        if(IC.keyType.contains("char"))
            IC.keyType = "char";

        if(  (IC.keyType.equals("int") || IC.keyType.equals("float")) && IC.keyLength!=4 )
        {
            System.out.println("Key MisMatch!!!!");
            return ;
        }
        IC.searchPath = new Stack<Integer>();

        IC.resultBlockSet = new ArrayList<Integer>();
        IC.resultRecordSet = new ArrayList<Integer>();

        IC.maxInnerNode =4087/(4+IC.keyLength);  // 4096-(char)type-(int)nodeList.size()-(int)nextBlock
        IC.maxLeafNode = 4087/(8+IC.keyLength);

        System.out.println("IndexManger::before operation,make sure:maxInnerNode="+IC.maxInnerNode+",maxLeafNode="+IC.maxLeafNode);
    }

    public static IndexContext get_index_context()
    {
        System.out.println("result block: "+IC.resultBlockSet.toString());
        System.out.println("result record: "+IC.resultRecordSet.toString());
        return IC;
    }

    public static int alloc_a_unused_block()
    {
        for(int i=0;i<2147483647;i++)
        {
            if(!IC.existBlock.contains(i))
            {
                IC.existBlock.add(i);
                return i;
            }
        }
        return -1;
    }



    // Find functions
    public static boolean find_all_index()             // will return all data
    {
        IC.resultRecordSet = new ArrayList<Integer>();
        IC.resultBlockSet = new ArrayList<Integer>();

        BplusNode tempNode = new BplusNode();
        BplusBlock BPB = get_leftmost_leafblock();
        if(BPB == null)
        {
            return false;
        }

        // now BPB should be the left most leaf block
        while(true)
        {
            for(int i=0;i<BPB.nodeList.size();i++)
            {
                tempNode = (BplusNode) BPB.nodeList.get(i);
                if(tempNode.recordNum < 0)
                {
                    System.out.println("IndexManager::find_all_index------->血崩");
                    return false;
                }
                IC.resultBlockSet.add(tempNode.blockNum);
                IC.resultRecordSet.add(tempNode.recordNum);
            }
            if(BPB.nextBlockNum == -1)
                break;

//            System.out.println("Loading block: "+BPB.nextBlockNum+".....");
            BPB = load_block(BPB.nextBlockNum,IC.searchPath.peek());
        }

        return true;
    }

    public static boolean find_index(String key)
    {
        BplusNode node = new BplusNode();
        if(IC.keyType=="int")
        {
            Integer i = Integer.parseInt(key);
            node.key = i;
        }
        else if(IC.keyType=="float")
        {
            Float f = Float.parseFloat(key);
            node.key =f;
        }
        else
            node.key = key;
        node.blockNum = -1;
        node.recordNum = -1;

        IC.keyNode = node.clone();

        return find_index(node);
    }

    public static boolean find_index(BplusNode queryNode)
    {
        IC.resultRecordSet = new ArrayList<Integer>();
        IC.resultBlockSet = new ArrayList<Integer>();

        System.out.println("\t\tIndexManager::find_index(BplusNode)-->正在查找"+queryNode.toString());

        IC.searchPath.push(-1);                     // push current block

        boolean find = false;

        BplusNode tempNode = new BplusNode();
        BplusBlock BPB = load_block(IC.rootBlock,-1);
        if(BPB == null)
        {
            System.out.println("\t\tIndexManager::find_all_index-->没找到root block？");
            return false;
        }
        if(BPB.nodeList.size() == 0)
        {
            System.out.println("\t\tB+ tree is empty");
            return false;
        }

//        System.out.println("Root Block has type: "+BPB.type);
        while(BPB.type != 1 )    // search and find the correct leaf block
        {
            IC.searchPath.push(BPB.currentBlock);
            System.out.println("\t\t\t\tfinding.....");

            int i;
            for(i=0;i<BPB.nodeList.size();i++)
            {
                tempNode = (BplusNode) BPB.nodeList.get(i);
                if(queryNode.compareTo(tempNode) < 0)
                    break;
            }

            if(i>=BPB.nodeList.size())
                BPB = load_block(BPB.nextBlockNum,BPB.currentBlock);
            else
                BPB = load_block(tempNode.blockNum,BPB.currentBlock);

            if(BPB==null)
            {
                System.out.println("IndexManager::find__index-->你应该看不到这行，load_block应该没错的");
                return false;
            }
        }

        // now BPB must be a leaf block
        IC.blockContainsKeyNode = BPB.currentBlock;
        int i;
        for(i=0;i<BPB.nodeList.size();i++)
        {
            tempNode = (BplusNode) BPB.nodeList.get(i);

            //todo
            if(tempNode.key == null)
                System.out.println("other node 's key is null");

            if(queryNode.compareTo(tempNode) == 0 )        //   find the data with same key
            {
                find = true;

                IC.keyNode.blockNum = tempNode.blockNum;
                IC.keyNode.recordNum = tempNode.recordNum;

                IC.resultBlockSet.add(tempNode.blockNum);
                IC.resultRecordSet.add(tempNode.recordNum);

//                break;
            }
        }

        if(find==false)
        {
            System.out.println("\t\t\t\tThe B+ tree does not contain the key!");
            IC.keyNode.blockNum = BPB.currentBlock;
            IC.keyNode.recordNum = -1;
        }
        return true;
    }

    public static boolean find_index_condition(String op, String value)
    {
        BplusNode node = new BplusNode();

        if(IC.keyType=="int")             // set key according the keytpye
        {
            Integer i = Integer.parseInt(value);
            node.key = i;
        }
        else if(IC.keyType=="float")
        {
            Float f = Float.parseFloat(value);
            node.key =f;
        }
        else
            node.key = value;

        node.blockNum = -1;
        node.recordNum = -1;

        IC.keyNode = node.clone();

        // parse the op
        if(op.equals("<"))
            return find_index_less_than(node, false);
        else if(op.equals("<="))
            return find_index_less_than(node,true);
        else if(op.equals(">"))                 // [ all ] - [ <= ]
        {
            find_index_less_than(node, true);

            ArrayList<Integer> tempBlockList = new ArrayList<Integer>(IC.resultBlockSet);
            ArrayList<Integer> tempRecordList = new ArrayList<Integer>(IC.resultRecordSet);

            find_all_index();

            System.out.println("[ <= ]");
            System.out.println("\t"+tempBlockList.toString());
            System.out.println("\t"+tempRecordList.toString());

            System.out.println("[ all ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            ArrayList<Integer> finalBlockSet = new ArrayList<Integer>();
            ArrayList<Integer> finalRecordSet = new ArrayList<Integer>();

            for(int i=0;i<IC.resultBlockSet.size();i++)
            {
                int b = IC.resultBlockSet.get(i);
                int r = IC.resultRecordSet.get(i);

                boolean drop = false;
                for(int j=0;j<tempBlockList.size();j++)
                {
                    int tb = tempBlockList.get(j);
                    int tr = tempRecordList.get(j);

                    if(tb==b && tr == r)
                    {
                        drop = true;
                        break;
                    }
                }
                if(!drop)
                {
                    finalBlockSet.add(b);
                    finalRecordSet.add(r);
                }

            }

            IC.resultBlockSet = finalBlockSet;
            IC.resultRecordSet = finalRecordSet;

            System.out.println("[ all ] - [ <= ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            return IC.resultBlockSet.size() == IC.resultRecordSet.size();
        }
        else if(op.equals(">="))      // [ all ] - [ < ]
        {
            find_index_less_than(node, false);

            ArrayList<Integer> tempBlockList = new ArrayList<Integer>(IC.resultBlockSet);
            ArrayList<Integer> tempRecordList = new ArrayList<Integer>(IC.resultRecordSet);

            System.out.println("[ < ]");
            System.out.println("\t"+tempBlockList.toString());
            System.out.println("\t"+tempRecordList.toString());

            find_all_index();

            System.out.println("[ all ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            ArrayList<Integer> finalBlockSet = new ArrayList<Integer>();
            ArrayList<Integer> finalRecordSet = new ArrayList<Integer>();

            for(int i=0;i<IC.resultBlockSet.size();i++)
            {
                int b = IC.resultBlockSet.get(i);
                int r = IC.resultRecordSet.get(i);

                boolean drop = false;
                for(int j=0;j<tempBlockList.size();j++)
                {
                    int tb = tempBlockList.get(j);
                    int tr = tempRecordList.get(j);

                    if(tb==b && tr == r)
                    {
                        drop = true;
                        break;
                    }
                }
                if(!drop)
                {
                    finalBlockSet.add(b);
                    finalRecordSet.add(r);
                }

            }

            IC.resultBlockSet = finalBlockSet;
            IC.resultRecordSet = finalRecordSet;

            System.out.println("[ all ] - [ < ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            return IC.resultBlockSet.size() == IC.resultRecordSet.size();

        }
        else if(op.equals("="))
            return find_index(node);
        else if(op.equals("!="))
        {
            find_index(node);

            ArrayList<Integer> tempBlockList = new ArrayList<Integer>(IC.resultBlockSet);
            ArrayList<Integer> tempRecordList = new ArrayList<Integer>(IC.resultRecordSet);

            System.out.println("[ = ]");
            System.out.println("\t"+tempBlockList.toString());
            System.out.println("\t"+tempRecordList.toString());

            find_all_index();

            System.out.println("[ all ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            ArrayList<Integer> finalBlockSet = new ArrayList<Integer>();
            ArrayList<Integer> finalRecordSet = new ArrayList<Integer>();

            for(int i=0;i<IC.resultBlockSet.size();i++)
            {
                int b = IC.resultBlockSet.get(i);
                int r = IC.resultRecordSet.get(i);

                boolean drop = false;
                for(int j=0;j<tempBlockList.size();j++)
                {
                    int tb = tempBlockList.get(j);
                    int tr = tempRecordList.get(j);

                    if(tb==b && tr == r)
                    {
                        drop = true;
                        break;
                    }
                }
                if(!drop)
                {
                    finalBlockSet.add(b);
                    finalRecordSet.add(r);
                }

            }

            IC.resultBlockSet = finalBlockSet;
            IC.resultRecordSet = finalRecordSet;

            System.out.println("[ all ] - [ = ]");
            System.out.println("\t"+IC.resultBlockSet.toString());
            System.out.println("\t"+IC.resultRecordSet.toString());

            return IC.resultBlockSet.size() == IC.resultRecordSet.size();
        }
        else
        {
            System.out.println("\t\tIndexManager::find_index_condition--->看不懂是"+op+"什么操作");
            return false;
        }
    }

    public static boolean find_index_less_than(BplusNode queryNode, boolean enableEqual)
    {
        IC.resultRecordSet = new ArrayList<Integer>();
        IC.resultBlockSet = new ArrayList<Integer>();

        BplusNode tempNode = new BplusNode();
        BplusBlock BPB = get_leftmost_leafblock();
        if(BPB == null)
        {
            return false;
        }

        // now BPB should be the left most leaf block
        while(true)
        {
            for(int i=0;i<BPB.nodeList.size();i++)
            {
                tempNode = (BplusNode) BPB.nodeList.get(i);
                if(tempNode.recordNum < 0)
                {
                    System.out.println("IndexManager::find_all_index------->血崩");
                    return false;
                }

                if(enableEqual  && tempNode.compareTo(queryNode)>0)
                    return true;
                if(!enableEqual && tempNode.compareTo(queryNode)>=0)
                    return true;

                IC.resultBlockSet.add(tempNode.blockNum);
                IC.resultRecordSet.add(tempNode.recordNum);
            }
            if(BPB.nextBlockNum == -1)
                break;

//            System.out.println("Loading block: "+BPB.nextBlockNum+".....");
            BPB = load_block(BPB.nextBlockNum,IC.searchPath.peek());
        }

        return true;
    }

    public static BplusBlock get_leftmost_leafblock()
    {
        IC.searchPath.push(-1);                     // push current block
        BplusNode tempNode = new BplusNode();
        BplusBlock BPB = load_block(IC.rootBlock,-1);
        if(BPB == null)
        {
            System.out.println("IndexManager::find_all_index-->没找到root block？");
            return null;
        }
        if(BPB.nodeList.size() == 0)
        {
            System.out.println("\t\tB+ tree is empty");
            return null;
        }

        while(BPB.type!=1)
        {
            tempNode = (BplusNode) BPB.nodeList.get(0);
            IC.searchPath.push(BPB.currentBlock);

            BPB = load_block(tempNode.blockNum,BPB.currentBlock);
            if(BPB==null)
            {
                System.out.println("IndexManager::find_all_index-->你应该看不到这行，load_block应该没错的");
                return null;
            }
        }

        return BPB;
    }






    // Insert Functions
    public static boolean insert_index(String key,int blockNum,int recordNum)
    {

        BplusNode node = new BplusNode();

        if(IC.keyType.equals("int"))
        {
            Integer i = Integer.parseInt(key);
            node.key = i;
        }
        else if(IC.keyType.equals("float"))
        {
            Float f = Float.parseFloat(key);
            node.key =f;
        }
        else
            node.key = key;

        node.blockNum= blockNum;
        node.recordNum=recordNum;

        IC.keyNode = node.clone();
        if(IC.keyNode == null)
            System.out.println("NULLLLLLLLLLLLLL");

        return insert_index(node);
    }

    public static boolean insert_index(BplusNode newNode)
    {
        if(IC.existBlock.size()==0)    // B+ tree is empty
        {
            System.out.println("\t\tIndexManager::insert_index--->B+ tree is empty,create a block 0 as root block!");
            BplusBlock BPB = new BplusBlock();    //create a new block as root block
            BPB.currentBlock=0;
            BPB.parentBlock=-1;
            BPB.type=1;
            BPB.nextBlockNum = -1;
            BPB.nodeList = new ArrayList<BplusNode>();

            BPB.nodeList.add(newNode.clone());

            IC.rootBlock = 0;
            IC.existBlock.add(0);

            System.out.println("\t\t\t\t下一块是："+BPB.nextBlockNum);

            return dump_block(BPB,false);
        }

        find_index(newNode);
        if( ! IC.existBlock.contains(IC.keyNode.blockNum) )
        {
            System.out.println("\t\tIndexManager::insert_index--->没道理啊");
            return false;
        }

        BplusBlock BPB = load_block(IC.keyNode.blockNum,IC.searchPath.peek());  // the top of searchPath is still the parent of BPB
        if(BPB == null)
        {
            System.out.println("IndexManager::insert_index-->不能读取要插入的叶子块位置");
            return false;
        }

        BPB.nodeList.add(newNode.clone());
        Collections.sort(BPB.nodeList,BplusNode.keyComp);

        if(BPB.nodeList.size() < IC.maxLeafNode)        /// just insert
        {
            System.out.println("\t\tIndexManager::insert_index--->叶子块"+BPB.currentBlock+"插入数据后节点数"+BPB.nodeList.size()+"未满"+IC.maxLeafNode+"，无需其他更新");
            return dump_block(BPB,false);
        }
        else       // need split the leaf node and update info
        {
            System.out.println("\t\tIndexManager::insert_index--->叶子块"+BPB.currentBlock+"插入数据后节点数为"+BPB.nodeList.size()+"，超过了"+IC.maxLeafNode+"，需要分裂新建一块");

            int n=BPB.nodeList.size();
            int sep = Math.floorDiv(n,2);
            BplusBlock newBPB = new BplusBlock();
            newBPB.currentBlock = alloc_a_unused_block();
            newBPB.parentBlock = BPB.parentBlock;  // don't need this info in further operation
            newBPB.nextBlockNum = BPB.nextBlockNum;
            newBPB.type = 1;
            newBPB.nodeList = new ArrayList( BPB.nodeList.subList( sep , n)  );

            BPB.nodeList = new ArrayList( BPB.nodeList.subList( 0 , sep ));
            BPB.nextBlockNum = newBPB.currentBlock;


            System.out.println("\t\tIndexManager::insert_index--->老块的下一块更新为："+BPB.nextBlockNum+"。新建的叶子块为："+newBPB.currentBlock+",下一块是："+newBPB.nextBlockNum);
            System.out.println("\t\t\t\t老块还有"+BPB.nodeList.size()+"个节点,新块有"+newBPB.nodeList.size()+"个节点，都没有超过"+IC.maxLeafNode);

            // create enough info for update parent block
            InsertUpInfo iuf = new InsertUpInfo();
            iuf.new_index_key = ((BplusNode)newBPB.nodeList.get(0)).key;
            iuf.old_index_key = ((BplusNode)BPB.nodeList.get(0)).key;
            iuf.new_BPB_block = newBPB.currentBlock;
            iuf.old_BPB_block = BPB.currentBlock;

            // store the leaf block
            if( dump_block(BPB,false)==false || dump_block(newBPB,false)==false)
            {
                System.out.println("IndexManager::insert_index--->存储叶子数据块失败");
                return false;
            }
            int parent = IC.searchPath.pop();    // make sure the pop of the IC.searchPath is always the current block's parent
            return insert_update_inner_block(parent, iuf, true);
        }
    }

    public static boolean insert_update_inner_block(int blockToUpdate, InsertUpInfo insertUpInfo, boolean fromLeaf)
    {
        if(insertUpInfo.old_BPB_block == IC.rootBlock)  // need create a new inner block as new root
        {
            System.out.println("\t\tIndexManager::insert_update_inner_block--->create a new block as root block!");
            BplusBlock newRootBlock = new BplusBlock();
            BplusNode tempNode = new BplusNode();

            tempNode.blockNum = insertUpInfo.old_BPB_block;
            tempNode.recordNum = -1;
            tempNode.key = fromLeaf?insertUpInfo.new_index_key:insertUpInfo.old_index_key;

            newRootBlock.currentBlock = alloc_a_unused_block();
            newRootBlock.parentBlock = -1;
            newRootBlock.nextBlockNum = insertUpInfo.new_BPB_block;
            newRootBlock.type = 0;
            newRootBlock.nodeList = new ArrayList();
            newRootBlock.nodeList.add(tempNode.clone());

            IC.rootBlock = newRootBlock.currentBlock;

            System.out.println("\t\t\t\t新建的root块"+IC.rootBlock+"：左子块为"+tempNode.blockNum+"，有一个索引key＝"+tempNode.key+",右子块为"+newRootBlock.nextBlockNum);

            return dump_block(newRootBlock,false);
        }
        if(blockToUpdate == -1)
        {
            System.out.println("IndexManager::insert_update_inner_block---->如果没出错，你应该看不到这行");
            return false;
        }

        BplusBlock BPB = load_block(blockToUpdate,IC.searchPath.peek());  // the parent block is not need for we have searchPath
        if(BPB == null)
        {
            System.out.println("IndexManager::insert_update_inner_block--->读取inner数据块出错");
            return false;
        }

        // insert the node first
        BplusNode minNode = (BplusNode) BPB.nodeList.get(0);
        BplusNode newNode = new BplusNode();

        newNode.blockNum = insertUpInfo.old_BPB_block;
        newNode.recordNum = -1;  //inner node
        newNode.key = fromLeaf?insertUpInfo.new_index_key:insertUpInfo.old_index_key;

        if(newNode.compareTo(minNode)<=0 )   // condition A
            minNode.blockNum = insertUpInfo.new_BPB_block;
        else   // the insertUpInfo.old_BPB_block is the right most child block
            BPB.nextBlockNum = insertUpInfo.new_BPB_block;

        BPB.nodeList.add(newNode.clone());
        Collections.sort(BPB.nodeList,BplusNode.keyComp);
        System.out.println("\t\tIndexManager::insert_update_inner_block--->更新了一个inner Node："+newNode.toString());

        if(BPB.nodeList.size() > IC.maxInnerNode)  // need split the inner node
        {
            System.out.println("\t\tTest:IndexManager::insert_update_inner_block--->need split the inner node");
            int n = BPB.nodeList.size();
            int sep = Math.floorDiv(n,2);

            BplusBlock newBPB = new BplusBlock();
            newBPB.currentBlock = alloc_a_unused_block();
            newBPB.parentBlock = BPB.parentBlock;
            newBPB.nextBlockNum = BPB.nextBlockNum;
            newBPB.type = 0;
            newBPB.nodeList = new ArrayList( BPB.nodeList.subList(sep,n));

            BPB.nodeList = new ArrayList( BPB.nodeList.subList(0,sep));
            BPB.nextBlockNum = ((BplusNode)BPB.nodeList.get(BPB.nodeList.size() -1)).blockNum;

            System.out.println("\t\t\t\t老块的下一块更新为"+BPB.nextBlockNum+"。新建块"+newBPB.currentBlock+"，下一块为"+newBPB.nextBlockNum);

            // create enough info for update parent block
            InsertUpInfo iuf = new InsertUpInfo();
            iuf.new_index_key = ((BplusNode)newBPB.nodeList.get(0)).key;  // update from inner don't need this actually
            iuf.old_index_key = ((BplusNode)BPB.nodeList.get(BPB.nodeList.size() -1)).key;
            iuf.new_BPB_block = newBPB.currentBlock;
            iuf.old_BPB_block = BPB.currentBlock;

            // store the leaf block
            if( dump_block(BPB,false)==false || dump_block(newBPB,false)==false)
            {
                System.out.println("IndexManager::insert_update_inner_block--->存储inner数据块失败");
                return false;
            }
            int parent = IC.searchPath.pop();
            return insert_update_inner_block(parent, iuf, true);
        }

        return dump_block(BPB,false);
    }






    // Delete Functions

    public static boolean delete_index_condition(String op,String value)
    {
        //todo
        return true;
    }

    public static boolean delete_index(String key)
    {
        BplusNode node = new BplusNode();

        if(IC.keyType=="int")
        {
            Integer i = Integer.parseInt(key);
            node.key = i;
        }
        else if(IC.keyType=="float")
        {
            Float f = Float.parseFloat(key);
            node.key =f;
        }
        else
            node.key = key;

        node.blockNum = -1;
        node.recordNum = -1;

        IC.keyNode = node.clone();

        return delete_index(node,false);
    }

    public static boolean delete_index(String key,int block,int record)
    {
        BplusNode node = new BplusNode();

        if(IC.keyType=="int")
        {
            Integer i = Integer.parseInt(key);
            node.key = i;
        }
        else if(IC.keyType=="float")
        {
            Float f = Float.parseFloat(key);
            node.key =f;
        }
        else
            node.key = key;

        node.blockNum = block;
        node.recordNum = record;

        IC.keyNode = node.clone();

        return delete_index(node,true);
    }

    public static boolean delete_index(BplusNode toDeleteNode,boolean accurate)  // accurate=true: only when block,record,key all match will delete
    {
        find_index(toDeleteNode);
        if( IC.keyNode.recordNum==-1)
        {
            System.out.println("IndexManager::delete_index---->太好了，要删除的节点不存在");
            return false;
        }

        BplusBlock BPB = load_block(IC.blockContainsKeyNode,IC.searchPath.peek());
        if(BPB == null)
        {
            System.out.println("IndexManager::delete_index--->读取数据块失败了");
            return false;
        }

        int currentBlock = BPB.currentBlock;

        // first delete the key on the leaf
        Iterator it = BPB.nodeList.iterator();
        while(it.hasNext())
        {
            BplusNode node = (BplusNode) it.next();
            if(toDeleteNode.compareTo( node )==0 )
            {
                if(accurate)
                {
                    System.out.println("\t\tIndexManager::the node "+toDeleteNode.toString()+" is deleted in accurate mode");
                    if (toDeleteNode.blockNum == node.blockNum && toDeleteNode.recordNum == node.recordNum)
                        it.remove();
                }
                else
                {
                    System.out.println("\t\tIndexManager::the node "+toDeleteNode.toString()+" is deleted in non-accurate mode");
                    it.remove();
                }
            }
        }

        Collections.sort(BPB.nodeList,BplusNode.keyComp);

        dump_block(BPB,false);

        return delete_update_block(currentBlock,true);
    }

    public static boolean delete_update_block(int blockToUpdate, boolean isLeaf)
    {
        int maxNode = (isLeaf)?IC.maxLeafNode:IC.maxInnerNode;

        if(blockToUpdate == -1)
            return true;
        int parent = IC.searchPath.pop();
        BplusBlock BPB = load_block(blockToUpdate,parent);
        if(BPB == null)
        {
            System.out.println("\t\tIndexManager::delete_update_block----->fetch BPB failed");
            return false;
        }

        System.out.println("\t\tIndexManager::delete_update_block----->当前更新的块是"+blockToUpdate+",节点数："+BPB.nodeList.size());

        if(BPB.currentBlock==IC.rootBlock)
        {
            System.out.println("\t\tRoot Block no need further update, just delete");

            if(BPB.nodeList.size()==0)
            {
                IC.existBlock.remove(BPB.currentBlock);
                dump_block(BPB,true);
            }
            else
                dump_block(BPB,false);

            return true;
        }


        if(BPB.currentBlock != IC.rootBlock)
        {
            int grandParent= (IC.searchPath.size()==0)?-1:IC.searchPath.peek();
            BplusBlock parentBlock = load_block(parent,grandParent);  // we don't need know the parent's parent
            if(parentBlock == null)
            {
                System.out.println("\t\tIndexManager::delete_update_block----->fetch parent node failed,parent ="+parent);
                return false;
            }
            int parentNodeNum = parentBlock.nodeList.size()+1;    // including nextBlockNum
            BplusNode leftSibParentNode = null;
            BplusNode currentParentNode = null;
            BplusNode rightSibParentNode = null;
            BplusNode tempNode = null;

            for(int i=0;i<parentBlock.nodeList.size();i++)          // find the left sibling block or right sibling block
            {
                tempNode = (BplusNode) parentBlock.nodeList.get(i);
//                System.out.println("\t\t\t\tIndexManager::delete_update_block----->Test the parent node: "+( (BplusNode)parentBlock.nodeList.get(i) ).toString());
//                System.out.println("\t\t\t\tIndexManager::delete_update_block----->Test the temp node: "+tempNode.toString());
                if(tempNode.blockNum == BPB.currentBlock)
                {
                    currentParentNode = tempNode;
                    if(i!=parentBlock.nodeList.size()-1)
                        rightSibParentNode = (BplusNode) parentBlock.nodeList.get(i+1);
                    break;
                }
                leftSibParentNode = tempNode;
//                System.out.println("\t\t\t\tIndexManager::delete_update_block----->Test the left sibling node: "+leftSibParentNode.toString());
            }


            BplusNode nodeToUpdate = (leftSibParentNode!=null)?leftSibParentNode:currentParentNode;

//            if(currentParentNode!=null)
//                System.out.println("\t\tIndexManager::delete_update_block-->\n\n\t\t\t\t\t\t\t\tThe current parent node "+currentParentNode.toString()+"\n\n\n");
//            if(leftSibParentNode!=null)
//                System.out.println("\t\tIndexManager::delete_update_block-->\n\n\t\t\t\t\t\t\t\tThe left sibling node "+leftSibParentNode.toString()+"\n\n\n");
//            System.out.println("\t\tIndexManager::delete_update_block-->\n\n\t\t\t\t\t\t\t\tThe node to update "+nodeToUpdate.toString()+"\n\n\n");

            if(BPB.nodeList.size() >= maxNode/2 )
            {
                if(leftSibParentNode!=null)
                {
                    nodeToUpdate.key = ((BplusNode) BPB.nodeList.get(0)).key;
                    System.out.println("\t\tIndexManager::delete_update_block--->No need check parent for this block not shrink,update the key to :" + nodeToUpdate.toString());
                }
                else
                    System.out.println("\t\tIndexManager::delete_update_block--->No need check parent for this block not shrink,no need update the key:"+nodeToUpdate.toString());

                dump_block(BPB,false);
                dump_block(parentBlock,false);

                return true;
            }

            if(leftSibParentNode != null )   // has left sibling inner block
            {
                BplusBlock leftBlock = load_block(leftSibParentNode.blockNum,parent);
                if(leftBlock == null)
                {
                    System.out.println("IndexManager::delete_update_block-----> fetch left block failllllllllled");
                    return false;
                }

                System.out.println("\t\tIndexManager::delete_update_block----->当前块有左兄弟块"+leftBlock.currentBlock+",节点数为"+leftBlock.nodeList.size());
                int old_leftBlockNodeNum = leftBlock.nodeList.size();
                // copy all the data of left block to BPB
                for(int i=0;i<leftBlock.nodeList.size();i++)
                {
                    tempNode = (BplusNode) leftBlock.nodeList.get(i);
                    BPB.nodeList.add(tempNode.clone());
                }
                int n = BPB.nodeList.size();
                Collections.sort(BPB.nodeList,BplusNode.keyComp);

                if( old_leftBlockNodeNum > maxNode/2 )         // borrow from left block
                {
                    leftBlock.nodeList = new ArrayList( BPB.nodeList.subList(0,n/2) );
                    BPB.nodeList = new ArrayList( BPB.nodeList.subList(n/2,n));

                    nodeToUpdate.key = ( (BplusNode) BPB.nodeList.get(0) ).key;

                    System.out.println("\t\t\tIndexManager::delete_update_block----->左子块节点足够，只需调整。调整后当前块有节点"+BPB.nodeList.size()+",左兄弟块有节点"+leftBlock.nodeList.size());
                    System.out.println("\t\t\tIndexManager::delete_update_block----->同时需要更新parent新节点："+nodeToUpdate.toString());
                    System.out.println("\t\t\tIndexManager::delete_update_block----->无需再检查上层块，因为上层块的节点数没有减少");

                    dump_block(leftBlock,false);
                    dump_block(BPB,false);
                    dump_block(parentBlock,false);

                    return true;
                }
                else                               ///           merge and delete left block
                {
                    Iterator iter = parentBlock.nodeList.iterator();
                    while(iter.hasNext())
                    {
                        if(nodeToUpdate.compareTo((BplusNode)iter.next())==0)
                        {
                            System.out.println("\t\t\tIndexManager::delete_update_block--->delete node: "+nodeToUpdate.toString()+" from parent "+parentBlock.currentBlock);
                            iter.remove();
                            break;
                        }
                    }
                    System.out.println("\t\t\tIndexManager::delete_update_block----->左兄弟块节点不足，需要merge，并摧毁左兄弟块");
                    Integer disappearBlock = new Integer(leftBlock.currentBlock);
                    IC.existBlock.remove(disappearBlock);
                    if(parentBlock.nodeList.size()==0)
                    {
                        System.out.println("\t\t\tIndexManager::delete_update_block--->parent(also root) no need exist,destroy it! New root block should be current Block: "+BPB.currentBlock);
                        disappearBlock = new Integer(parentBlock.currentBlock);
                        IC.existBlock.remove(disappearBlock);
                        IC.rootBlock = BPB.currentBlock;

                        dump_block(parentBlock,true);       // destroy root block
                    }
                    else
                        dump_block(parentBlock,false);

                    dump_block(leftBlock,true);    // destroy left sibling block
                    dump_block(BPB,false);

                    return delete_update_block(parent,false);
                }

            }
            else      // only has right sibling block. if no left sibling block ,this must be true for current block is not root block
            {
                BplusBlock rightBlock = new BplusBlock();

                if(isLeaf && BPB.nextBlockNum!=-1)
                    rightBlock = load_block(BPB.nextBlockNum, parent);
                else if(rightSibParentNode!=null)
                    rightBlock = load_block(rightSibParentNode.blockNum,parent);
                else
                    rightBlock = load_block(parentBlock.nextBlockNum,parent);

                if(rightBlock == null)
                {
                    System.out.println("IndexManager::delete_update_block----->fetch right block failed");
                    return false;
                }

                System.out.println("\t\tIndexManager::delete_update_block----->有右兄弟块"+rightBlock.currentBlock+",节点数："+rightBlock.nodeList.size());
                int old_rightNodeNum = rightBlock.nodeList.size();
                // copy all the data of BPB to rightBLock
                for(int i=0;i<BPB.nodeList.size();i++)
                {
                    tempNode = (BplusNode) BPB.nodeList.get(i);
                    rightBlock.nodeList.add(tempNode.clone());
                }
                int n = rightBlock.nodeList.size();
                Collections.sort(rightBlock.nodeList,BplusNode.keyComp);

                if( old_rightNodeNum > maxNode/2 )
                {
                    BPB.nodeList = new ArrayList( rightBlock.nodeList.subList(0,n/2) );
                    rightBlock.nodeList = new ArrayList( rightBlock.nodeList.subList(n/2,n));

                    nodeToUpdate.key = ( (BplusNode)rightBlock.nodeList.get(0) ).key;

                    System.out.println("\t\t\tIndexManager::delete_update_block----->右兄弟节点充足，调整后当前块节点数"+BPB.nodeList.size()+"，右兄弟节点数"+rightBlock.nodeList.size());
                    System.out.println("\t\t\tIndexManager::delete_update_block----->同时更新节点到parent：" + nodeToUpdate.toString());
                    System.out.println("\t\t\tIndexManager::delete_update_block----->无需再检查上层块，因为上层块的节点数没有减少");

                    dump_block(rightBlock,false);
                    dump_block(BPB,false);
                    dump_block(parentBlock,false);

                    return true;
                }
                else         // merge and delete the BPB block, leave the right block
                {
                    Iterator iter = parentBlock.nodeList.iterator();
                    while(iter.hasNext())
                    {
                        if(nodeToUpdate.compareTo((BplusNode)iter.next())==0)
                        {
                            iter.remove();
                            break;
                        }
                    }
                    System.out.println("\t\t\tIndexManager::delete_update_block----->右兄弟块节点不足，将merge并摧毁当前块");

                    Integer disappearBlock = new Integer(BPB.currentBlock);
                    IC.existBlock.remove(disappearBlock);
                    dump_block(BPB,true);                // destroy current block

                    if(parentBlock.nodeList.size()==0)
                    {
                        System.out.println("\t\t\tIndexManager::delete_update_block--->parent(also root) no need exist,destroy it! New root block should be right sibling Block: "+rightBlock.currentBlock);
                        disappearBlock = new Integer(parentBlock.currentBlock);
                        IC.existBlock.remove(disappearBlock);
                        IC.rootBlock = rightBlock.currentBlock;

                        dump_block(parentBlock,true);       // destroy root block
                    }
                    else
                        dump_block(parentBlock,false);


                    dump_block(rightBlock,false);


                    return delete_update_block(parent,false);
                }
            }
        }

        if(BPB!=null)
            dump_block(BPB,false);

        return true;
    }

    public static boolean purge_index()
    {
        IC.rootBlock = 0;
        IC.existBlock.clear();

        System.out.println("\t\tindex is purged");

        return true;
    }







    // value check functions
    public static boolean type_value_verification(String type,String value)
    {
        if(type.matches("char\\([0-9]+\\)"))
        {
            String temp=type.replaceAll("char\\(","");

            temp=temp.replaceAll("\\)","");
            int length= Integer.parseInt(temp);
            if(value.length()>length)
            {
                System.out.println("CatalogManager::AttributeMetaFile::type_value_verification-->length overflow:>"+length);
                return false;
            }
            return true;
        }
        else if(type.equals("int") && value.matches("^[-]*[0-9]+$"))
        {
//            System.out.println("CatalogManager::AttributeMetaFile::type_value_verification-->Test:the "+value+" is int");
            return true;
        }
        else if(type.equals("float") && value.matches("^[-]*[0-9]+[.]*[0-9]*$"))
        {
//            System.out.println("CatalogManager::AttributeMetaFile::type_value_verification-->Test:the " + value + " is float");
            return true;
        }

        return false;
    }

    public static boolean check_condition_match(AttributeMetaFile amf, String attrValue, String op,String value)
    {
        if(!type_value_verification(amf.type,value))
            return false;

        if(amf.type.equals("int"))
        {
            Integer op1= Integer.parseInt(attrValue);
            Integer op2= Integer.parseInt(value);

            return compare(op1,op,op2);
        }
        else if(amf.type.equals("float"))
        {
            Float op1 = Float.parseFloat(attrValue);
            Float op2 = Float.parseFloat(value);
            return compare(op1,op,op2);
        }
        else
        {
            return compare(attrValue,op,value);
        }
    }

    public static boolean compare(int op1,String op,int op2)
    {
        if(op.equals("<"))
            return op1<op2;
        else if(op.equals(">"))
            return op1>op2;
        else if(op.equals("<="))
            return op1<=op2;
        else if(op.equals(">="))
            return op1>=op2;
        else if(op.equals("!="))
            return op1!=op2;
        else if(op.equals("="))
            return op1==op2;

        return false;
    }

    public static boolean compare(float op1,String op , float op2)
    {
        if(op.equals("<"))
            return op1<op2;
        else if(op.equals(">"))
            return op1>op2;
        else if(op.equals("<="))
            return op1<=op2;
        else if(op.equals(">="))
            return op1>=op2;
        else if(op.equals("!="))
            return op1!=op2;
        else if(op.equals("="))
            return op1==op2;

        return false;
    }

    public static boolean compare(String op1, String op, String op2)
    {
        int status=op1.compareToIgnoreCase(op2);

        if(op.equals(">"))
            return status>0;
        else if(op.equals("<"))
            return status<0;
        else if(op.equals(">="))
            return status>=0;
        else if(op.equals("<=") )
            return status<=0;
        else if(op.equals("!="))
            return status!=0;
        else if(op.equals("="))
            return status==0;

        return false;
    }









    // store and load block
    public static BplusBlock load_block(int currentBlock, int parentBlock)
    {
        int nodeNum = 0;

        BplusBlock BPB = new BplusBlock();
        BPB.nodeList = new ArrayList<BplusNode>();
        BPB.currentBlock = currentBlock;
        BPB.parentBlock = parentBlock;

        ByteBuffer wrapped;
        byte [] tempByte;
        Integer tempInt;
        Float tempFloat;
        String tempStr;
        BplusNode tempNode;

        byte [] blockData = BufferManager.get_block_readonly(IC.filename, currentBlock);
        if(blockData==null)
        {
            System.out.println("IndexManager::load_block--->get block return null");
            return null;
        }

//        System.out.println("\nthe byte stream fetched:");
//        for(int i=0;i<blockData.length;i++)
//            System.out.print(blockData[i]);

        // get the block type
        tempByte = Arrays.copyOfRange(blockData,0,4);
        blockData = Arrays.copyOfRange(blockData,4,blockData.length);
        wrapped = ByteBuffer.wrap(tempByte);
        tempInt = wrapped.getInt();
        BPB.type = tempInt;

//        System.out.println("\nthe byte stream removed block type:");
//        for(int i=0;i<blockData.length;i++)
//            System.out.print(blockData[i]);

        // get the node num
        tempByte = Arrays.copyOfRange(blockData,0,4);
        blockData = Arrays.copyOfRange(blockData,4,blockData.length);
        wrapped = ByteBuffer.wrap(tempByte);
        tempInt = wrapped.getInt();
        nodeNum = tempInt;

        if(nodeNum == 0)
        {
            System.out.println("\t\tIndexManager::load_block-->The block "+currentBlock+" has no content, something before is wrong!");
            return null;
        }

//        System.out.println("\nthe byte stream removed node num:");
//        for(int i=0;i<blockData.length;i++)
//            System.out.print(blockData[i]);

        // get the node list
        for(int i=0;i<nodeNum;i++)         // load the node list
        {
            tempNode = new BplusNode();
            tempNode.key = new Object();
            // get the key
            tempByte = Arrays.copyOfRange(blockData,0,IC.keyLength);
            blockData=Arrays.copyOfRange(blockData,IC.keyLength,blockData.length);

            wrapped = ByteBuffer.wrap(tempByte);
            tempInt = new Integer( wrapped.getInt() );

            wrapped = ByteBuffer.wrap(tempByte);
            tempFloat = new Float( wrapped.getFloat() );

            tempStr = new String(tempByte,java.nio.charset.StandardCharsets.UTF_8).trim();

            if(IC.keyType.equals("int"))
                tempNode.key = tempInt;
            else if(IC.keyType.equals("float"))
                tempNode.key = tempFloat;
            else if(IC.keyType.equals("char"))
                tempNode.key = tempStr;
            else
            {
                System.out.println("IndexManager::load_block--->undefined key type!");
                return null;
            }

            if(tempNode.key == null)
                System.out.println("load_block load null key!");

            // get the blockNum
            tempByte = Arrays.copyOfRange(blockData,0,4);
            blockData = Arrays.copyOfRange(blockData,4,blockData.length);
            wrapped = ByteBuffer.wrap(tempByte);
            tempInt = wrapped.getInt();
            tempNode.blockNum = tempInt;

            // if is leaf block, get the recordNum
            tempNode.recordNum = -1;
            if(BPB.type==1)
            {
                tempByte = Arrays.copyOfRange(blockData,0,4);
                blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                wrapped = ByteBuffer.wrap(tempByte);

                tempInt = wrapped.getInt();
                tempNode.recordNum = tempInt;
//                System.out.println("IndexManager::load_block---->load a new leaf node:"+tempNode.toString());
            }
            BPB.nodeList.add(tempNode.clone());
        }

//        System.out.println("\nthe byte stream removed node list:");
//        for(int i=0;i<blockData.length;i++)
//            System.out.print(blockData[i]);

        // get the nextBLockNum
        tempByte = Arrays.copyOfRange(blockData,0,4);
        blockData = Arrays.copyOfRange(blockData,4,blockData.length);
        wrapped = ByteBuffer.wrap(tempByte);
        tempInt = wrapped.getInt();
        BPB.nextBlockNum = tempInt;

//        System.out.println("\nTest BPB.nextBlockNum:"+BPB.nextBlockNum);
//        for(int i =0;i<tempByte.length;i++)
//            System.out.print(tempByte[i]);

        BufferManager.return_block(IC.filename,currentBlock,false);

//        System.out.println();
//        System.out.println("***************************************IndexManager::load_block***************************************");
//        System.out.println("Test: check what we have loaded:");
//        System.out.println("Block:"+BPB.currentBlock+"\ttype:"+BPB.type);
//        System.out.println("\tnodeList.size():"+BPB.nodeList.size());
//        System.out.println("\t---------------------------------------------------");
//        for(int i=0;i<BPB.nodeList.size();i++)
//        {
//            tempNode = (BplusNode) BPB.nodeList.get(i);
//            System.out.println("\t"+tempNode.toString());
//        }
//        System.out.println("\t---------------------------------------------------");
//        System.out.println("\tnextBlock:"+BPB.nextBlockNum);
//        System.out.println();
        return BPB;
    }

    public static boolean dump_block(BplusBlock BPB,boolean destroyBlock)
    {
        if(BPB==null)
        {
            System.out.println("IndexManager::dump_block---->空的存个毛啊！");
            return false;
        }
        // sort the node list
        Collections.sort(BPB.nodeList,BplusNode.keyComp);

//        BPB.nodeList.size() = BPB.nodeList.size();
//        System.out.println("Test: check the block before dump it:");
//        System.out.println("Block:"+BPB.currentBlock+"\ttype:"+BPB.type);
//        System.out.println("\tnodeList.size():"+BPB.nodeList.size());
//        System.out.println("\t---------------------------------------------------");
//        for(int i=0;i<BPB.nodeList.size();i++)
//        {
//            BplusNode tempNode = (BplusNode) BPB.nodeList.get(i);
//            System.out.println("\t"+tempNode.toString());
//        }
//        System.out.println("\t---------------------------------------------------");
//        System.out.println("\tnextBlock:"+BPB.nextBlockNum);
//        System.out.println();


        ByteArrayOutputStream BAOS = new ByteArrayOutputStream();
        ArrayList<byte []> to_dump = new ArrayList<byte[]>();
        byte [] tempByte = new byte[4096];

        // dump block type//todo
        tempByte = ByteBuffer.allocate(4).putInt(BPB.type).array();
        to_dump.add(tempByte);
//        System.out.println("dump the block type, to_dump size:"+to_dump.size());
        // dump the nodeList.size()
        tempByte = ByteBuffer.allocate(4).putInt(BPB.nodeList.size()).array();
        to_dump.add(tempByte);
//        System.out.println("Test BPB.nodeList.size():");
//        for(int i =0;i<tempByte.length;i++)
//            System.out.print(tempByte[i]);

//        System.out.println("dump the nodeList.size(), to_dump size:"+to_dump.size());

        // dump the node list
        for(int i=0;i<BPB.nodeList.size();i++)
        {
            BplusNode node = (BplusNode)BPB.nodeList.get(i);
            // dump the key
            if(IC.keyType.equals("int"))
                tempByte = ByteBuffer.allocate(4).putInt((Integer) node.key).array();
            else if(IC.keyType.equals("float"))
                tempByte = ByteBuffer.allocate(4).putFloat((Float) node.key).array();
            else if(IC.keyType.equals("char"))
            {
                String str = (String) node.key;
                while(str.length() < IC.keyLength)
                    str+='\0';
                tempByte = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            to_dump.add(tempByte);
            // dump the blockNum
            tempByte = ByteBuffer.allocate(4).putInt(node.blockNum).array();
            to_dump.add(tempByte);
            if(BPB.type==1)
            {
                tempByte = ByteBuffer.allocate(4).putInt(node.recordNum).array();
                to_dump.add(tempByte);
            }
        }

        // dump the nextBlockNum
        tempByte = ByteBuffer.allocate(4).putInt(BPB.nextBlockNum).array();
        to_dump.add(tempByte);

//        System.out.println("\nTest BPB.nextBlockNum:"+BPB.nextBlockNum);
//        for(int i =0;i<tempByte.length;i++)
//            System.out.print(tempByte[i]);


//        ByteBuffer wrapped = ByteBuffer.wrap(tempByte);
//        Integer t = wrapped.getInt();
//        System.out.println("\nConvert back-->the BPB.nextBlockNum="+t);

        if(   ( BPB.type==0  && (to_dump.size() != (BPB.nodeList.size()*2 + 3)) ) ||  (  BPB.type==1  && (to_dump.size() != (BPB.nodeList.size()*3 + 3))  )  )      // block type + nodeList.size() + nodelist + nextBlock
        {
            System.out.println("the size of to_dump is:"+to_dump.size());
            System.out.println("the size of nodeList is:"+BPB.nodeList.size());
            System.out.println("IndexManager::dump_block---->别忙着存，这里数据量不匹配");
            return false;
        }
        // dump the data
        for(int i=0;i<to_dump.size();i++)
        {
            try {
                BAOS.write(to_dump.get(i));
            }
            catch(IOException e){
                e.printStackTrace();
            }

        }
        byte [] blockData = BufferManager.get_block_readwrite(IC.filename,BPB.currentBlock);
        if(blockData==null)
        {
            System.out.println("IndexManager::dump_block---》拿不到数据块的写权限");
            return false;
        }

        tempByte = destroyBlock?(new byte[4096]):BAOS.toByteArray();           // if destroyBlock=true, write empty content to block
        byte [] newBlockData = new byte[4096];

        System.arraycopy(tempByte,0,newBlockData,0,tempByte.length);
        System.arraycopy(newBlockData,0,blockData,0,4096);
//        for(int i=0;i<tempByte.length;i++)
//            System.out.print(tempByte[i]);
//
//        System.out.println("\nnewBLockData:");
//        for(int i=0;i<newBlockData.length;i++)
//            System.out.print(newBlockData[i]);
//
//        System.out.println("\nupdated blockData");
//        for(int i=0;i<blockData.length;i++)
//            System.out.print(blockData[i]);

        BufferManager.return_block(IC.filename,BPB.currentBlock,true);
        blockData = BufferManager.get_block_readonly(IC.filename,BPB.currentBlock);

        for(int i=0;i<blockData.length;i++)
        {
            if(blockData[i]!=newBlockData[i])
            {
                System.out.println("IndexManager::dump_block--->对比检查：写回索引块数据时失败了");
                return false;
            }
        }
        BufferManager.return_block(IC.filename,BPB.currentBlock,false);

        BPB =null;   // free the data

        return true;
    }









    // debug Functions
    public static void print_all_leaf()
    {
        IC.searchPath.push(-1);                     // push current block
        BplusNode tempNode = new BplusNode();
        BplusBlock BPB = load_block(IC.rootBlock,-1);
        if(BPB == null)
        {
            System.out.println("\t\tIndexManager::find_all_index-->没找到root block？");
            return ;
        }
        if(BPB.nodeList.size() == 0)
        {
            System.out.println("\t\tB+ tree is empty");
            return;
        }

        while(BPB.type!=1)
        {
            tempNode = (BplusNode)BPB.nodeList.get(0);
            IC.searchPath.push(BPB.currentBlock);

            BPB = load_block(tempNode.blockNum,BPB.currentBlock);
            if(BPB==null)
            {
                System.out.println("\t\tIndexManager::find_all_index-->你应该看不到这行，load_block应该没错的");
                return ;
            }
        }
        System.out.println("****************************************************************");
        System.out.println("根块号："+IC.rootBlock);
        System.out.print("所有块号："+IC.existBlock.toString());
        System.out.println("\n****************************************************************");
        // now BPB should be the left most leaf block
        System.out.println("以下是所有从左到右叶子节点的所有数据：");
        while(true)
        {
            print_block(BPB);

            if(BPB.nextBlockNum == -1)
                break;

            BPB = load_block(BPB.nextBlockNum,IC.searchPath.peek());
        }
    }

    public static void print_bplus_tree()
    {
        System.out.println("################################################################################################################################################");
        System.out.println("Root="+IC.rootBlock);
        System.out.println("include: "+IC.existBlock.toString());
        System.out.println();
        ArrayList<Integer> queue = new ArrayList<Integer>();

        IC.searchPath.push(-1);

        BplusBlock BPB = load_block(IC.rootBlock,-1);
        if(BPB == null)
        {
            System.out.println("IndexManager::find_all_index-->没找到root block？");
            return ;
        }
        if(BPB.nodeList.size() == 0)
        {
            System.out.println("\t\tB+ tree is empty");
            return;
        }

        while(true)
        {
            print_block(BPB);

            if(BPB.type == 0)   // leaf block no need to enqueue
            {
                for(int i=0;i<BPB.nodeList.size();i++)
                {
                    queue.add(  (  (BplusNode)BPB.nodeList.get(i)  ).blockNum );
                }
                queue.add(BPB.nextBlockNum);
            }

            Iterator iter = queue.iterator();
            if(iter.hasNext())
            {
                Integer nextBlock = (Integer) iter.next();
                if(nextBlock == -1)
                    break;

                System.out.println("going to print:"+nextBlock);
                BPB = load_block(nextBlock,-1);
                if(BPB==null)
                {
                    System.out.println("IndexManager::find_all_index-->你应该看不到这行，load_block应该没错的");
                    return ;
                }

                iter.remove();
            }
            else
                break;

        }
        System.out.println("################################################################################################################################################");
    }

    public static void print_block(BplusBlock BPB)
    {
        BplusNode tempNode;

        System.out.println();
        System.out.println("Block:"+BPB.currentBlock+"\ttype:"+BPB.type);
        System.out.println("\tnodeList.size():"+BPB.nodeList.size()+"\tkeyLength:"+IC.keyLength);
        System.out.println("\t---------------------------------------------------");
        for(int i=0;i<BPB.nodeList.size();i++)
        {
            tempNode = (BplusNode) BPB.nodeList.get(i);

            System.out.println("\t"+tempNode.toString());
        }
        System.out.println("\t---------------------------------------------------");
        System.out.println("\tnextBlock:"+BPB.nextBlockNum);
    }

    public static void print_block(int block)
    {
        BplusBlock BPB = load_block(block,-1);
        if(BPB==null)
        {
            System.out.println("IndexManager::print_block(int)-->你应该看不到这行，load_block应该没错的");
            return ;
        }

        print_block(BPB);
    }
}
