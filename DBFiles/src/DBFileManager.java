import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by zieng on 10/12/15.
 */
public class DBFileManager
{
    public static boolean read(String filename,int block_num, int size, byte[] data)
    {
        //debug
//        System.out.println("\t\topen "+filename+"'s "+block_num+" block");
        try
        {
            File file = new File(filename);
            if(!file.exists())
                file.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            RandomAccessFile fr = new RandomAccessFile(filename,"r");
            byte[] temp = new byte[4096];

            fr.seek(block_num*size);   //Sets the file-pointer offset, measured from the beginning of this file, at which the next read or write occurs.
            fr.read(temp);  //Reads up to temp.length bytes of data from this file into an array of bytes.
            System.arraycopy(temp,0,data,0,temp.length);
            fr.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean write(String filename,int block_num,int size,byte[] data)
    {
//        System.out.println("\t\twrite "+filename+"'s "+block_num+" block");
        try
        {
            File file = new File(filename);
            if(!file.exists())
                file.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            RandomAccessFile fr = new RandomAccessFile(filename,"rw");
            byte[] temp = new byte[4096];

            System.arraycopy(data,0,temp,0,temp.length);
            fr.seek(block_num*size);
            fr.write(temp);  //write temp.length bytes of data to this file from an array of bytes.
            fr.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return true;
    }
}
