import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import BplusBlock.TYPE;

/**
 * Created by zieng on 10/16/15.
 */

/*
the raw data block structure is:
struct block
{
    char type;         // type='l'-->leaf block or type='i'--->non-leaf block
    int nodeNum;
    node [] nodeList;
    int nextBlock;
    char endFlag;   // endFlag='$'--->it's the last leaf node(block) ; or endFlag='>'---> has right siblings
                    // inner block don't need the endFlag
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

    private static BplusBlock BPB;   // current block

    public static void set_index_context(IndexContext index_context)  // index_context at least need:filename,keyLength,keyType,rootBlock,existBlock
    {
        IC=index_context;

        IC.keyValue="";
        IC.keyBlockNum = -1;
        IC.keyRecordNum = -1;

        IC.maxInnerNode = 4087/(4+IC.keyLength);  // 4096-(char)type-(int)nodeNum-(int)nextBlock
        IC.maxLeafNode = 4086/(8+IC.keyLength);   // 4096 -(char)type-(int)nodeNum-(int)nextBloc-(char)endFlag
    }

    public static IndexContext get_index_context()
    {
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
    
    public static boolean insert_insert(String key,int blockNum,int recordNum)
    {
        IC.keyValue=key;
        IC.keyRecordNum = recordNum;
        IC.keyBlockNum = blockNum;

        return insert_index(IC.keyValue);
    }

    public static boolean insert_index(String key)
    {
        IC.keyValue=key;

        BPB = null;
        if(IC.existBlock.size()==0)     // the B+ tree is empty,create a new leaf block as a root block
        {
            IC.rootBlock=0;
            IC.existBlock.add(0);

            BPB = new BplusBlock();
            BPB.currentBlock=0;
            BPB.parentBlock=-1;
            BPB.nextBlockNum=0;
            BPB.type=TYPE.LEAF;
            BPB.nodeNum=1;
            BPB.endFlag=true;

            dump_block(BPB);
            BPB = load_block( IC.rootBlock, IC.rootBlock);  // get the root block
            if(BPB == null)
            {
                System.out.println("IndexManager::insert_index---->读取索引块出错啦");
                return false;
            }
            IC.searchPath.push(-1);
        }
        else
        {
            find_index(IC.keyValue);   // will set the BPB to be the leaf node contains or next to the key
        }

        // Now the BPB is leaf block
        if(BPB.nodeNum < IC.maxLeafNode)      // just insert into the leaf block ,that's all. simple.
        {
            // add the node into nodeList
            if(IC.keyType.equals("int"))
            {
                Integer k = Integer.parseInt(IC.keyValue);

                BplusLeafNode_int node_int = new BplusLeafNode_int();
                node_int.key= k;
                node_int.blockNum=IC.keyBlockNum;
                node_int.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_int);
            }
            else if(IC.keyType.equals("float"))
            {
                Float k = Float.parseFloat(IC.keyValue);

                BplusLeafNode_float node_float = new BplusLeafNode_float();
                node_float.key=k;
                node_float.blockNum=IC.keyBlockNum;
                node_float.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_float);
            }
            else if(IC.keyType.equals("char"))
            {
                String k = IC.keyValue;

                BplusLeafNode_char node_char = new BplusLeafNode_char();
                node_char.key=k;
                node_char.blockNum=IC.keyBlockNum;
                node_char.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_char);
            }
            dump_block(BPB);
        }
        else      // split the leaf node:only when there are 1 block will affect the root directly
        {
            int n=IC.maxLeafNode+1;
            BplusBlock BPB_new = new BplusBlock();

            BPB_new.type=TYPE.LEAF;
            BPB_new.parentBlock=BPB.parentBlock;
            BPB_new.nextBlockNum=BPB.nextBlockNum;
            for(int i=0;i<2147483647;i++)
            {
                if(!IC.existBlock.contains(i))
                {
                    BPB_new.currentBlock=i;
                    IC.existBlock.add(i);
                    break;
                }
            }
            BPB.nextBlockNum=BPB_new.currentBlock;
            BPB.nodeNum = Math.floorDiv(n,2)+1;
            BPB_new.nodeNum = n - BPB.nodeNum;

            // add the node into nodeList
            if(IC.keyType.equals("int"))
            {
                Integer k = Integer.parseInt(IC.keyValue);

                BplusLeafNode_int node_int = new BplusLeafNode_int();
                node_int.key= k;
                node_int.blockNum=IC.keyBlockNum;
                node_int.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_int);

                Collections.sort(BPB.nodeList,BplusLeafNode_int.keyComp);

                BPB_new.nodeList = new ArrayList();
                BPB_new.nodeList = new ArrayList(   BPB.nodeList.subList( (int)Math.floorDiv(n,2)+1,BPB.nodeList.size() )  );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(0,(int)Math.floorDiv(n,2)));

                node_int = (BplusLeafNode_int) BPB_new.nodeList.get(0);
                BplusInnerNode_int up_node_new = new BplusInnerNode_int();
                up_node_new.key=node_int.key;
                up_node_new.blockNum=BPB_new.currentBlock;

                node_int = (BplusLeafNode_int) BPB.nodeList.get(0);
                BplusInnerNode_int up_node_old = new BplusInnerNode_int();
                up_node_old.key=node_int.key;
                up_node_old.blockNum=BPB.currentBlock;

                // store the leaf block
                dump_block(BPB_new);
                dump_block(BPB);

                int parent=IC.searchPath.pop();   // IC.searchPath at least has a -1,won't be empty.
                return insert_inner_node(up_node_old,up_node_new,parent,false);

            }
            else if(IC.keyType.equals("float"))
            {
                Float k = Float.parseFloat(IC.keyValue);

                BplusLeafNode_float node_float = new BplusLeafNode_float();
                node_float.key=k;
                node_float.blockNum=IC.keyBlockNum;
                node_float.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_float);

                Collections.sort(BPB.nodeList,BplusLeafNode_float.keyComp);

                BPB_new.nodeList = new ArrayList();
                BPB_new.nodeList = new ArrayList(   BPB.nodeList.subList( Math.floorDiv(n,2)+1,BPB.nodeList.size() )  );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(0,Math.floorDiv(n,2)));

                node_float = (BplusLeafNode_float) BPB_new.nodeList.get(0);
                BplusInnerNode_float up_node_new = new BplusInnerNode_float();
                up_node_new.key=node_float.key;
                up_node_new.blockNum=BPB_new.currentBlock;

                node_float = (BplusLeafNode_float) BPB.nodeList.get(0);
                BplusInnerNode_float up_node_old = new BplusInnerNode_float();
                up_node_old.key=node_float.key;
                up_node_old.blockNum=BPB.currentBlock;

                // store the leaf block
                dump_block(BPB_new);
                dump_block(BPB);

                int parent=IC.searchPath.pop();   // IC.searchPath at least has a -1,won't be empty.
                return insert_inner_node(up_node_old,up_node_new,parent,false);

            }
            else if(IC.keyType.equals("char"))
            {
                String k = IC.keyValue;

                BplusLeafNode_char node_char = new BplusLeafNode_char();
                node_char.key=k;
                node_char.blockNum=IC.keyBlockNum;
                node_char.recordNum=IC.keyRecordNum;

                BPB.nodeList.add(node_char);

                Collections.sort(BPB.nodeList,BplusLeafNode_char.keyComp);

                BPB_new.nodeList = new ArrayList();
                BPB_new.nodeList = new ArrayList(   BPB.nodeList.subList( Math.floorDiv(n,2)+1,BPB.nodeList.size() )  );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(0,Math.floorDiv(n,2)));

                node_char = (BplusLeafNode_char) BPB_new.nodeList.get(0);
                BplusInnerNode_char up_node_new = new BplusInnerNode_char();
                up_node_new.key=node_char.key;
                up_node_new.blockNum=BPB_new.currentBlock;

                node_char = (BplusLeafNode_char) BPB.nodeList.get(0);
                BplusInnerNode_char up_node_old = new BplusInnerNode_char();
                up_node_old.key=node_char.key;
                up_node_old.blockNum=BPB.currentBlock;

                // store the leaf block
                dump_block(BPB_new);
                dump_block(BPB);

                int parent=IC.searchPath.pop();   // IC.searchPath at least has a -1,won't be empty.
                return insert_inner_node(up_node_old,up_node_new,parent,false);
            }
        }

        //todo

        return true;

    }

    public static boolean insert_inner_node(Object node_old,Object node_new,int block, boolean fromInner)   // when need create a inner block,need node_node.
    {
        // the block contains node_old used to be root,now we will make a new root
        if(block == -1)       // need create a new inner block as new root block
        {
            BplusBlock newRootBlock = new BplusBlock();
            for(int i=0;i<2147483647;i++)
            {
                if(!IC.existBlock.contains(i))
                {
                    newRootBlock.currentBlock=i;
                    newRootBlock.parentBlock=-1;
                    IC.existBlock.add(i);
                    IC.rootBlock=i;
                    break;
                }
            }
            newRootBlock.nodeNum=1;
            newRootBlock.nodeList = new ArrayList();      //todo
            //newRootBlock.nodeList.add(node_old);          // I'm not sure java's cast works as I expect

            // add the node to the new root block and set the nextBlockNum
            if(IC.keyType.equals("int"))
            {
                BplusInnerNode_int node = (BplusInnerNode_int) node_old;
                newRootBlock.nodeList.add(node);
                node = (BplusInnerNode_int) node_new;
                newRootBlock.nextBlockNum=node.blockNum;
            }
            else if(IC.keyType.equals("float"))
            {
                BplusInnerNode_float node = (BplusInnerNode_float) node_old;
                newRootBlock.nodeList.add(node);
                node = (BplusInnerNode_float) node_new;
                newRootBlock.nextBlockNum=node.blockNum;
            }
            else if(IC.keyType.equals("char"))
            {
                BplusInnerNode_char node = (BplusInnerNode_char) node_old;
                newRootBlock.nodeList.add(node);
                node = (BplusInnerNode_char) node_new;
                newRootBlock.nextBlockNum=node.blockNum;
            }

            return dump_block(newRootBlock);
        }

        int parent = IC.searchPath.pop();   // because block!=-1, means IC.searchPath at least has -1 in stack
        BPB = load_block(block,parent);
        if(BPB == null)
        {
            System.out.println("IndexManager::insert_inner_index---->读取索引块出错啦");
            return false;
        }
        if(BPB.nodeNum < IC.maxInnerNode)    // just insert into the block, loves it!!
        {
            int index;
            if(IC.keyType.equals("int"))
            {
                BplusInnerNode_int node = (BplusInnerNode_int) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_int.keyComp);

                if(fromInner)  //should swap the node's blockNum and the next-node's blockNum
                {
                    index = BPB.nodeList.indexOf(node_new);
                    if(index==BPB.nodeList.size()-1)   /// the new node to be insert is at the end--->new child block should be nextBlockNum
                    {
                        int temp = BPB.nextBlockNum;
                        BPB.nextBlockNum= node.blockNum;
                        node.blockNum = temp;
                    }
                    else
                    {
                        int temp = node.blockNum;
                        node.blockNum = ((BplusInnerNode_int)BPB.nodeList.get(index+1)).blockNum;
                        ((BplusInnerNode_int)BPB.nodeList.get(index+1)).blockNum = temp;
                    }
                }
            }
            else if(IC.keyType.equals("float"))
            {
                BplusInnerNode_float node = (BplusInnerNode_float) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_float.keyComp);

                if(fromInner)  //should swap the node's blockNum and the next-node's blockNum
                {
                    index = BPB.nodeList.indexOf(node_new);
                    if(index==BPB.nodeList.size()-1)   /// the new node to be insert is at the end--->new child block should be nextBlockNum
                    {
                        int temp = BPB.nextBlockNum;
                        BPB.nextBlockNum= node.blockNum;
                        node.blockNum = temp;
                    }
                    else
                    {
                        int temp = node.blockNum;
                        node.blockNum = ((BplusInnerNode_float)BPB.nodeList.get(index+1)).blockNum;
                        ((BplusInnerNode_float)BPB.nodeList.get(index+1)).blockNum = temp;
                    }
                }
            }
            else if(IC.keyType.equals("char"))
            {
                BplusInnerNode_char node = (BplusInnerNode_char) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_char.keyComp);

                if(fromInner)  //should swap the node's blockNum and the next-node's blockNum
                {
                    index = BPB.nodeList.indexOf(node_new);
                    if(index==BPB.nodeList.size()-1)   /// the new node to be insert is at the end--->new child block should be nextBlockNum
                    {
                        int temp = BPB.nextBlockNum;
                        BPB.nextBlockNum= node.blockNum;
                        node.blockNum = temp;
                    }
                    else
                    {
                        int temp = node.blockNum;
                        node.blockNum = ((BplusInnerNode_char)BPB.nodeList.get(index+1)).blockNum;
                        ((BplusInnerNode_char)BPB.nodeList.get(index+1)).blockNum = temp;
                    }
                }
            }

            return dump_block(BPB);
        }
        else       ///  need split the inner block, hate it!
        {
            int n=IC.maxInnerNode+1;
            BplusBlock BPB_new = new BplusBlock();
            for(int i=0;i<2147483647;i++)
            {
                if(!IC.existBlock.contains(i))
                {
                    BPB_new.currentBlock=i;
                    BPB_new.parentBlock=parent;
                    IC.existBlock.add(i);
                    break;
                }
            }
            BPB_new.nextBlockNum = BPB.nextBlockNum;

            if(IC.keyType.equals("int"))
            {
                BplusInnerNode_int node = (BplusInnerNode_int) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_int.keyComp);

                BPB_new.nodeList = new ArrayList( BPB.nodeList.subList( Math.floorDiv(n,2)+1 , BPB.nodeList.size()) );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(  0 , Math.floorDiv(n,2)    ));

                BplusInnerNode_int up_node_old = new BplusInnerNode_int();
                BplusInnerNode_int up_node_new = new BplusInnerNode_int();

                node = (BplusInnerNode_int) BPB.nodeList.get(0);
                up_node_old.key = node.key;
                up_node_old.blockNum=BPB.currentBlock;

                node = (BplusInnerNode_int) BPB.nodeList.get(Math.floorDiv(n,2));
                up_node_new.key = node.key;
                up_node_new.blockNum = BPB_new.currentBlock;

                // store the current inner block
                dump_block(BPB_new);
                dump_block(BPB);

                return insert_inner_node(up_node_old,up_node_new,parent,true);

            }
            else if(IC.keyType.equals("float"))
            {
                BplusInnerNode_float node = (BplusInnerNode_float) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_float.keyComp);

                BPB_new.nodeList = new ArrayList( BPB.nodeList.subList( Math.floorDiv(n,2)+1 , BPB.nodeList.size()) );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(  0 , Math.floorDiv(n,2)    ));

                BplusInnerNode_float up_node_old = new BplusInnerNode_float();
                BplusInnerNode_float up_node_new = new BplusInnerNode_float();

                node = (BplusInnerNode_float) BPB.nodeList.get(0);
                up_node_old.key = node.key;
                up_node_old.blockNum=BPB.currentBlock;

                node = (BplusInnerNode_float) BPB.nodeList.get(Math.floorDiv(n,2));
                up_node_new.key = node.key;
                up_node_new.blockNum = BPB_new.currentBlock;

                // store the current inner block
                dump_block(BPB_new);
                dump_block(BPB);

                return insert_inner_node(up_node_old,up_node_new,parent,true);

            }
            else if(IC.keyType.equals("char"))
            {
                BplusInnerNode_char node = (BplusInnerNode_char) node_new;
                BPB.nodeList.add(node);
                BPB.nodeNum++;
                Collections.sort(BPB.nodeList,BplusInnerNode_char.keyComp);

                BPB_new.nodeList = new ArrayList( BPB.nodeList.subList( Math.floorDiv(n,2)+1 , BPB.nodeList.size()) );
                BPB.nodeList = new ArrayList(  BPB.nodeList.subList(  0 , Math.floorDiv(n,2)    ));

                BplusInnerNode_char up_node_old = new BplusInnerNode_char();
                BplusInnerNode_char up_node_new = new BplusInnerNode_char();

                node = (BplusInnerNode_char) BPB.nodeList.get(0);
                up_node_old.key = node.key;
                up_node_old.blockNum=BPB.currentBlock;

                node = (BplusInnerNode_char) BPB.nodeList.get(Math.floorDiv(n,2));
                up_node_new.key = node.key;
                up_node_new.blockNum = BPB_new.currentBlock;

                // store the current inner block
                dump_block(BPB_new);
                dump_block(BPB);

                return insert_inner_node(up_node_old,up_node_new,parent,true);
            }
        }
        return true;
    }

    public static boolean delete_index(String key)
    {
        IC.keyValue=key;
        if(IC.existBlock.size()==0)
        {
            System.out.println("IndexManager::delete_key---->伟大的B＋树是空的，还不能删除");
            return false;
        }
        find_index(IC.keyValue);
        if(IC.keyRecordNum == -1)
        {
            System.out.println("IndexManager::delete_key---->太好了！没有你要找的索引，不用执行恶心的B＋树删除操作了");
            return false;
        }

        if(BPB.nodeNum > IC.maxLeafNode/2)  // just delete the node , simple
        {

        }
        else      /// should merge it's sibling
        {

        }

        return true;
    }

    public static boolean find_index(String key)
    {
        IC.keyValue=key;
        load_block(IC.rootBlock,-1);  // get the root block
        IC.searchPath.push(-1);
        while(BPB.type!=TYPE.LEAF)
        {
            if(IC.keyType.equals("int"))
            {
                int i=0;
                Integer k=Integer.parseInt(IC.keyValue);
                BplusInnerNode_int node_int;
                for(i=0;i<BPB.nodeNum;i++)
                {
                    node_int = (BplusInnerNode_int) BPB.nodeList.get(i);
                    if(k<node_int.key)
                        break;
                }
                if(i>=BPB.nodeNum) // in the right most child
                {
                    node_int = (BplusInnerNode_int) BPB.nodeList.get(BPB.nodeNum);
                }
                else
                {
                    node_int = (BplusInnerNode_int) BPB.nodeList.get(i);
                }
                IC.searchPath.push(BPB.currentBlock);
                BPB = load_block(node_int.blockNum, BPB.currentBlock);
                if(BPB == null)
                {
                    System.out.println("IndexManager::find_index---->读取索引块出错啦");
                    return false;
                }
            }
            else if(IC.keyType.equals("float"))
            {
                int i=0;
                Float k = Float.parseFloat(IC.keyValue);
                BplusInnerNode_float node_float;
                for(i=0;i<BPB.nodeNum;i++)
                {
                    node_float = (BplusInnerNode_float) BPB.nodeList.get(i);
                    if(k<node_float.key)
                        break;
                }
                if(i>=BPB.nodeNum) // in the right most child
                {
                    node_float = (BplusInnerNode_float) BPB.nodeList.get(BPB.nodeNum);
                }
                else
                {
                    node_float = (BplusInnerNode_float) BPB.nodeList.get(i);
                }
                IC.searchPath.push(BPB.currentBlock);
                BPB = load_block(node_float.blockNum,BPB.currentBlock);
                if(BPB == null)
                {
                    System.out.println("IndexManager::find_index---->读取索引块出错啦");
                    return false;
                }
            }
            else if(IC.keyType.equals("char"))
            {
                int i=0;
                String k=IC.keyValue;
                BplusInnerNode_char node_char;
                for(i=0;i<BPB.nodeNum;i++)
                {
                    node_char = (BplusInnerNode_char) BPB.nodeList.get(i);
                    if(k.compareTo(node_char.key)<0)
                        break;
                }
                if(i>=BPB.nodeNum) // in the right most child
                {
                    node_char = (BplusInnerNode_char) BPB.nodeList.get(BPB.nodeNum);
                }
                else
                {
                    node_char = (BplusInnerNode_char) BPB.nodeList.get(i);
                }
                IC.searchPath.push(BPB.currentBlock);
                BPB = load_block(node_char.blockNum,BPB.currentBlock);
                if(BPB == null)
                {
                    System.out.println("IndexManager::find_index---->读取索引块出错啦");
                    return false;
                }
            }
        }

        // now BPB is leaf block
        int i=0;
        if(IC.keyType.equals("int"))
        {
            Integer k= Integer.parseInt(IC.keyValue);
            BplusLeafNode_int node_int;
            for(i=0;i<BPB.nodeNum;i++)
            {
                node_int = (BplusLeafNode_int) BPB.nodeList.get(i);
                if(k == node_int.key)
                {
                    IC.keyBlockNum = node_int.blockNum;
                    IC.keyRecordNum= node_int.recordNum;
                    break;
                }
            }
        }
        else if(IC.keyType.equals("float"))
        {
            Float k = Float.parseFloat(IC.keyValue);
            BplusLeafNode_float node_float;
            for(i=0;i<BPB.nodeNum;i++)
            {
                node_float = (BplusLeafNode_float) BPB.nodeList.get(i);
                if(k == node_float.key)
                {
                    IC.keyBlockNum = node_float.blockNum;
                    IC.keyRecordNum= node_float.recordNum;
                    break;
                }
            }
        }
        else if(IC.keyType.equals("char"))
        {
            String k=IC.keyValue;
            BplusLeafNode_char node_char;
            for(i=0;i<BPB.nodeNum;i++)
            {
                node_char = (BplusLeafNode_char) BPB.nodeList.get(i);
                if(k.equals(node_char.key))
                {
                    IC.keyBlockNum=node_char.blockNum;
                    IC.keyRecordNum=node_char.recordNum;
                    break;
                }
            }
        }
        if(i>=BPB.nodeNum)     // not found,return info for insert
        {
            IC.keyBlockNum=BPB.currentBlock;
            IC.keyRecordNum=-1;
        }


        return true;
    }

    public static int find_left_sibling(int currentBlock)
    {
        int left =-1;
        BplusBlock BPB = load_block(IC.rootBlock,-1);

        while (BPB.type!=TYPE.LEAF)
        {
            if(IC.keyType.equals("int"))
            {
                BplusInnerNode_int node = (BplusInnerNode_int) BPB.nodeList.get(0);
                BPB = load_block(node.blockNum,BPB.currentBlock);
            }
            else if(IC.keyType.equals("float"))
            {
                BplusInnerNode_float node = (BplusInnerNode_float) BPB.nodeList.get(0);
                BPB = load_block(node.blockNum,BPB.currentBlock);
            }
            else if(IC.keyType.equals("char"))
            {
                BplusInnerNode_char node = (BplusInnerNode_char) BPB.nodeList.get(0);
                BPB = load_block(node.blockNum,BPB.currentBlock);
            }
        }

        // now BPB is leaf block
        left=BPB.currentBlock;
        while(left!=currentBlock)
        {

        }

        return left;
    }

    public static BplusBlock load_block(int currentBlock, int parentBlock)
    {
        BplusBlock BPB;

        byte [] temp;
        byte [] blockData = BufferManager.get_block_readonly(IC.filename, currentBlock);
        if(blockData==null)
        {
            System.out.println("IndexManager::load_block--->get block return null");
            return null;
        }

        // determine which type the block is
        temp = Arrays.copyOfRange(blockData,0,1);
        char blockType = (char) (temp[0] & 0xFF);
        blockData = Arrays.copyOfRange(blockData,1,blockData.length);

        BPB = new BplusBlock();
        BPB.type=(blockType=='l')? TYPE.LEAF:TYPE.INNER;
        BPB.currentBlock=currentBlock;
        BPB.parentBlock=parentBlock;

        temp = Arrays.copyOfRange(blockData,0,4);
        ByteBuffer wrapped = ByteBuffer.wrap(temp);
        Integer intValue = wrapped.getInt();
        BPB.nodeNum=intValue;
        blockData = Arrays.copyOfRange(blockData,4,blockData.length);

        // structure the node data
        BPB.nodeList = new ArrayList();
        if(blockType=='i')   // i means inner block or root block
        {
            if(BPB.nodeNum > IC.maxInnerNode)
            {
                System.out.println("IndexManager::load_block--->块中存放的node数量超过限制！");
                return null;
            }

            // get nodeList from raw block
            if(IC.keyType.equals("int"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusInnerNode_int node_int = new BplusInnerNode_int();

                     // get key
                     temp = Arrays.copyOfRange(blockData,0,4);
                     wrapped = ByteBuffer.wrap(temp);
                     intValue = wrapped.getInt();
                     node_int.key=intValue;
                     blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                     // get value
                     temp = Arrays.copyOfRange(blockData,0,4);
                     wrapped = ByteBuffer.wrap(temp);
                     intValue = wrapped.getInt();
                     node_int.blockNum = intValue;
                     blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                     // add to the nodeList
                     BPB.nodeList.add(node_int);
                }
                Collections.sort(BPB.nodeList,BplusInnerNode_int.keyComp);
            }
            else if(IC.keyType.equals("float"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusInnerNode_float node_float = new BplusInnerNode_float();

                    //get key
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    Float floatValue = wrapped.getFloat();
                    node_float.key=floatValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // get value
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_float.blockNum = intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // add to the nodeList
                    BPB.nodeList.add(node_float);
                }
                Collections.sort(BPB.nodeList,BplusInnerNode_float.keyComp);
            }
            else if(IC.keyType.equals("char"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusInnerNode_char node_char= new BplusInnerNode_char();

                    //get key
                    temp = Arrays.copyOfRange(blockData,0,IC.keyLength);
                    String strValue = new String(temp,java.nio.charset.StandardCharsets.UTF_8);
                    node_char.key = strValue.trim();
                    blockData=Arrays.copyOfRange(blockData,IC.keyLength,blockData.length);
                    // get value
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_char.blockNum = intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // add to the nodeList
                    BPB.nodeList.add(node_char);
                }
                Collections.sort(BPB.nodeList,BplusInnerNode_char.keyComp);
            }
            // get the next block num(or the right sibling block num)
            temp = Arrays.copyOfRange(blockData,0,4);
            wrapped = ByteBuffer.wrap(temp);
            intValue = wrapped.getInt();
            BPB.nextBlockNum=intValue;
            blockData = Arrays.copyOfRange(blockData,4,blockData.length);
        }
        else if(blockType=='l')   // l means leaf block
        {
            if(BPB.nodeNum > IC.maxLeafNode)
            {
                System.out.println("IndexManager::load_block--->块中存放的node数量超过限制！");
                return null;
            }

            // get the nodeList
            if(IC.keyType.equals("int"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusLeafNode_int node_int = new BplusLeafNode_int();

                    // get key
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_int.key=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // get value:blockNum
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_int.blockNum=intValue;
                    blockData = Arrays.copyOfRange(blockData, 4, blockData.length);
                    // get value:recordNum
                    temp = Arrays.copyOfRange(blockData, 0, 4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_int.recordNum=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);

                    // add to the nodeList
                    BPB.nodeList.add(node_int);
                }
                Collections.sort(BPB.nodeList,BplusLeafNode_int.keyComp);
            }
            else if(IC.keyType.equals("float"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusLeafNode_float node_float = new BplusLeafNode_float();

                    // get key
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    Float floatValue = wrapped.getFloat();
                    node_float.key=floatValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // get value:blockNum
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_float.blockNum=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // get value:recordNum
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_float.recordNum=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);

                    // add to the nodeList
                    BPB.nodeList.add(node_float);
                }
                Collections.sort(BPB.nodeList,BplusLeafNode_float.keyComp);
            }
            else if(IC.keyType.equals("char"))
            {
                for(int i=0;i<BPB.nodeNum;i++)
                {
                    BplusLeafNode_char node_char = new BplusLeafNode_char();

                    //get key
                    temp = Arrays.copyOfRange(blockData,0,IC.keyLength);
                    String strValue = new String(temp,java.nio.charset.StandardCharsets.UTF_8);
                    node_char.key = strValue.trim();
                    blockData=Arrays.copyOfRange(blockData,IC.keyLength,blockData.length);
                    // get value:blockNum
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_char.blockNum=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);
                    // get value:recordNum
                    temp = Arrays.copyOfRange(blockData,0,4);
                    wrapped = ByteBuffer.wrap(temp);
                    intValue = wrapped.getInt();
                    node_char.recordNum=intValue;
                    blockData = Arrays.copyOfRange(blockData,4,blockData.length);

                    // add to the nodeList
                    BPB.nodeList.add(node_char);
                }
                Collections.sort(BPB.nodeList,BplusLeafNode_char.keyComp);
            }
            // get the next block num(or the right sibling block num)
            temp = Arrays.copyOfRange(blockData,0,4);
            wrapped = ByteBuffer.wrap(temp);
            intValue = wrapped.getInt();
            BPB.nextBlockNum=intValue;
            blockData = Arrays.copyOfRange(blockData,4,blockData.length);
            // get the endFlag
            temp = Arrays.copyOfRange(blockData,0,1);
            char end = (char) (temp[0] & 0xFF);
            BPB.endFlag = (end=='$')?true:false;
            blockData = Arrays.copyOfRange(blockData,1,blockData.length);
        }

        return BPB;
    }

    public static boolean dump_block(BplusBlock BPB)
    {
        if(BPB==null)
        {
            System.out.println("IndexManager::dump_block---->你绝对看不到这一行字");
            return false;
        }

        char blockType=(BPB.type==TYPE.LEAF)?'l':'i';
        int nodeNum=BPB.nodeNum;
        byte [] temp;
        ByteArrayOutputStream byteOutPutStream = new ByteArrayOutputStream();

        // set the block type
        temp = ByteBuffer.allocate(1).putChar(blockType).array();
        try
        {
            byteOutPutStream.write(temp);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        // set the nodeNum
        temp = ByteBuffer.allocate(4).putInt(nodeNum).array();
        try
        {
            byteOutPutStream.write(temp);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        // set the nodeList
        if(BPB.type==TYPE.INNER)
        {
            if(IC.keyType.equals("int"))
            {
                // sort the node
                Collections.sort(BPB.nodeList,BplusInnerNode_int.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusInnerNode_int node_int = (BplusInnerNode_int) BPB.nodeList.get(i);

                    // set key and blockNum
                    byte[] key = ByteBuffer.allocate(4).putInt(node_int.key).array();
                    byte[] blockNum = ByteBuffer.allocate(4).putInt(node_int.blockNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else if(IC.keyType.equals("float"))
            {
                // sort thd node
                Collections.sort(BPB.nodeList,BplusInnerNode_float.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusInnerNode_float node_float = (BplusInnerNode_float) BPB.nodeList.get(i);
                    // set key and blockNum
                    byte[] key = ByteBuffer.allocate(4).putFloat(node_float.key).array();
                    byte[] blockNum = ByteBuffer.allocate(4).putInt(node_float.blockNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else if(IC.keyType.equals("char"))
            {
                // sort thd node
                Collections.sort(BPB.nodeList,BplusInnerNode_char.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusLeafNode_char node_char = (BplusLeafNode_char) BPB.nodeList.get(i);

                    String str=node_char.key;
                    while(str.length()<IC.keyLength)
                        str+='\0';
                    byte [] key = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte [] blockNum = ByteBuffer.allocate(4).putInt(node_char.blockNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            // set the next block
            temp = ByteBuffer.allocate(4).putInt(BPB.nextBlockNum).array();
            try
            {
                byteOutPutStream.write(temp);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        else if(BPB.type==TYPE.LEAF)
        {
            if(IC.keyType.equals("int"))
            {
                // sort thd node
                Collections.sort(BPB.nodeList,BplusLeafNode_int.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusLeafNode_int node_int = (BplusLeafNode_int) BPB.nodeList.get(i);
                    //set the key and blockNum and recordNum
                    byte [] key = ByteBuffer.allocate(4).putInt(node_int.key).array();
                    byte [] blockNum = ByteBuffer.allocate(4).putInt(node_int.blockNum).array();
                    byte [] recordNum = ByteBuffer.allocate(4).putInt(node_int.recordNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                        byteOutPutStream.write(recordNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else if(IC.keyType.equals("float"))
            {
                // sort thd node
                Collections.sort(BPB.nodeList,BplusLeafNode_float.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusLeafNode_float node_float =(BplusLeafNode_float) BPB.nodeList.get(i);
                    //set the key and blockNum and recordNum
                    byte [] key = ByteBuffer.allocate(4).putFloat(node_float.key).array();
                    byte [] blockNum = ByteBuffer.allocate(4).putInt(node_float.blockNum).array();
                    byte [] recordNum = ByteBuffer.allocate(4).putInt(node_float.recordNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                        byteOutPutStream.write(recordNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else if(IC.keyType.equals("char"))
            {
                // sort thd node
                Collections.sort(BPB.nodeList,BplusLeafNode_char.keyComp);

                for(int i=0;i<nodeNum;i++)
                {
                    BplusLeafNode_char node_char = (BplusLeafNode_char) BPB.nodeList.get(i);
                    // set the key and blockNum and recordNum
                    String str=node_char.key;
                    while(str.length()<IC.keyLength)
                        str+='\0';
                    byte [] key = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte [] blockNum = ByteBuffer.allocate(4).putInt(node_char.blockNum).array();
                    byte [] recordNum = ByteBuffer.allocate(4).putInt(node_char.recordNum).array();
                    try
                    {
                        byteOutPutStream.write(key);
                        byteOutPutStream.write(blockNum);
                        byteOutPutStream.write(recordNum);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            // set the next block
            temp = ByteBuffer.allocate(4).putInt(BPB.nextBlockNum).array();
            // set the endFlag
            char end = (BPB.endFlag==true)?'i':'l';
            byte[] endFlag = ByteBuffer.allocate(1).putChar(end).array();

            try
            {
                byteOutPutStream.write(temp);
                byteOutPutStream.write(endFlag);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        byte [] newBlockData = byteOutPutStream.toByteArray();
        while(newBlockData.length<4096)                         /// append 0x0000_0000 to fill the 4096
            newBlockData[newBlockData.length]=(byte)0;
        byte [] oldBlockData = BufferManager.get_block_readwrite(IC.filename,BPB.currentBlock);
        System.arraycopy(newBlockData,0,oldBlockData,0,4096);
        // check if write successfully
        oldBlockData = BufferManager.get_block_readonly(IC.filename, BPB.currentBlock);
        if(!oldBlockData.equals(newBlockData))
        {
            System.out.println("IndexManager::dump_block--->对比检查：写回索引块数据时失败了");
            return false;
        }

        BPB = null;   // free memory

        return true;
    }

}
