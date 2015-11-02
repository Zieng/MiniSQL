/**
 * Created by zieng on 10/4/15.
 */
public class Block
{
//    static int blockSize;  // static member shared by all instance of the class
    private boolean isDirty;
    private boolean isLocked;
    private int borrow;   // how many borrow get the block
    private boolean isWriting;
    private String file;   // the file the block belongs to.
    private int blockID;
    private byte [] content;   // 4092 byte for record, 4 byte for pointer
    private long latestUseTime;

    public Block()
    {
        borrow =0;
        isDirty=false;
        isLocked=false;
        content = new byte[4096];
        blockID = -1;
        file= new String();
        latestUseTime = -1;
    }

    public boolean load_content(String filename, int block_num)
    {
        DBFileManager.read(filename,block_num,4096,content);
        file=filename;
        blockID=block_num;
        borrow = 0;
        isDirty = false;
        isLocked = false;
        isWriting = false;
        latestUseTime = System.currentTimeMillis();

        return true;
    }

    public boolean match_block(String filename, int block_num)
    {
        if(file.equals(filename) && blockID==block_num)
            return true;
        return false;
    }

    public boolean replace_block(String filename,int block_num)
    {
//        System.out.println("replace block...");
        if(isDirty)
        {
            if(DBFileManager.write(file,blockID,4096,content)==false)
                return false;
        }
        return load_content(filename,block_num);
    }

    public boolean can_replace()
    {
        if(isLocked==false && borrow <=0 && isWriting==false)
            return true;
        return false;
    }

    public boolean pin()
    {
        isLocked = true;
        return true;
    }

    public boolean unpin()
    {
        isLocked=false;
        return true;
    }

    public byte[] get_readonly()
    {
        borrow++;
        latestUseTime = System.currentTimeMillis();
        return content;
    }

    public byte[] get_readwrite()
    {
        borrow++;
        isDirty=true;
        isWriting=true;
        latestUseTime = System.currentTimeMillis();
        return content;
    }

    public boolean return_data(boolean writeDone)
    {
        if(writeDone==true)
            isWriting= false;
        borrow--;
        return true;
    }

    public boolean is_writing()
    {
        return isWriting;
    }

    public boolean close_block()
    {
        if(isDirty)
        {
            if(DBFileManager.write(file,blockID,4096,content)==false)
                return false;
        }
        return true;
    }

    public void print_info()
    {
        System.out.println("filename:" + file + " block:" + blockID + " borrow:" + borrow + " pined:" + isLocked + " writed:" + isDirty);
    }










}
