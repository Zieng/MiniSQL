import java.util.ArrayList;

/**
 * Created by zieng on 10/10/15.
 */
public class BufferManager
{
    private static ArrayList<Block> blockList = new ArrayList<Block>();
    private static int size;
    private static int used;
    private static int position;

    public BufferManager(int buffer_size)
    {
        used=0;
        position=0;
        size=buffer_size;
        for(int i=0;i<size;i++)
            blockList.add(new Block());
    }

    public static byte[] get_block_readonly(String filename, int block_num)
    {
        if(block_num < 0)
        {
            System.out.println("BufferManager::上层必然出错，否则怎么会叫我去拿一个负数的块");
            return null;
        }

        for(int i=0;i<used;i++)  // 先在buffer中找一下有没有
        {
            if(blockList.get(i).match_block(filename,block_num))
                return blockList.get(i).get_readonly();
        }
        if(used<size)    // 没有，且buffer有空余，load
        {
            blockList.get(used).load_content(filename,block_num);
            used++;
            return blockList.get(used-1).get_readonly();
        }
        else    // 没有，且buffer已满，替换
        {
            int maxtry = size * 2;
            for (int tryed = 0;tryed < maxtry;tryed++) {
                if (++position == size) {
                    position = 0;
                }
//                System.out.println("position= "+position);
                if (blockList.get(position).can_replace()) {
                    blockList.get(position).replace_block(filename, block_num);
                    return blockList.get(position).get_readonly();
                }
            }
        }
        return null;
    }

    public static byte[] get_block_readwrite(String filename, int block_num)
    {
        if(block_num < 0)
        {
            System.out.println("BufferManager::上层必然出错，否则怎么会叫我去拿一个负数的块");
            return null;
        }

        for(int i=0;i<used;i++)  // 先在buffer中找一下有没有
        {
            if(blockList.get(i).match_block(filename,block_num))
            {
                if (blockList.get(i).is_writing())
                    return null;
                return blockList.get(i).get_readwrite();
            }
        }
        if(used<size)    // 没有，且buffer有空余，load
        {
//            System.out.println("load a new block from file");
            blockList.get(used).load_content(filename,block_num);
            used++;
            return blockList.get(used-1).get_readwrite();
        }
        else    // 没有，且buffer已满，替换
        {
            int maxtry = size * 2;
            for (int tryed = 0;tryed < maxtry;tryed++)
            {
                if (++position == size)
                {
                    position = 0;
                }
//                System.out.println("position= "+position);
                if (blockList.get(position).can_replace())
                {
                    blockList.get(position).replace_block(filename, block_num);
                    return blockList.get(position).get_readwrite();
                }
            }
        }
        return null;
    }

    public static boolean pin_block(String filename, int block_num)
    {
        for(int i=0;i<size;i++)
        {
            if(blockList.get(i).match_block(filename,block_num))
            {
                blockList.get(i).pin();
                return true;
            }
        }
        return false;
    }

    public static boolean unpin_block(String filename,int block_num)
    {
        for(int i=0;i<size;i++)
        {
            if(blockList.get(i).match_block(filename,block_num))
            {
                blockList.get(i).unpin();
                return true;
            }
        }
        return false;
    }

    public static boolean return_block(String filename, int block_num,boolean writeDone)
    {
        for(int i=0;i<size;i++)
        {
            if(blockList.get(i).match_block(filename,block_num))
            {
                blockList.get(i).return_data(writeDone);
                return true;
            }
        }
        return false;
    }

    public static boolean close_buffer()
    {
        for(int i=0;i<used;i++)
        {
            if(! blockList.get(i).close_block() )
                return false;
        }
        return true;
    }

    public static void print_buffer()
    {
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("=======Buffer Debug Report=========");
        System.out.println("buffer size: "+size+", used block: "+used);
        System.out.println("===used===");
        int i=0;
        for (;i < used;i++) {
            System.out.println(" block "+i+" : ");
            blockList.get(i).print_info();
            System.out.println();
        }
        System.out.println("===unused===");
        for (;i < size;i++) {
            System.out.println(" block "+i+" : ");
            blockList.get(i).print_info();
            System.out.println();
        }
        System.out.println("=======End of Report=======");
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
