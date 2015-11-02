import java.io.File;
import java.io.IOException;

/**
 * Created by zieng on 10/13/15.
 */
public class TestBufferManager
{
    public static void main(String [] args)
    {
        BufferManager buffer = new BufferManager(2);
        byte [][] text = new byte[11][];

        text[0]=buffer.get_block_readwrite("data_1.dbfile",0);
        if(text[0]==null)
        {
            System.out.println("get_block return null");
        }
//        System.out.println(text[0][0]);
        text[0][0] = 1;
//        System.out.println(text[0][0]);
        buffer.return_block("data_1.dbfile",0,true);
        buffer.print_buffer();


        text[0] = buffer.get_block_readwrite("data_1.dbfile",1);
        if(text[0]==null)
        {
            System.out.println("get_block return null");
        }
        text[0][0] = (byte)0xFF;
        buffer.return_block("data_1.dbfile",1,true);
        buffer.print_buffer();

        text[1] = buffer.get_block_readwrite("data_2.dbfile",0);
        if(text[1]==null)
        {
            System.out.println("get_block return null");
        }
        text[1][0] = 10;
//        buffer.return_block("data_2.dbfile",0,text[1],true);
        buffer.return_block("data_2.dbfile",0,true);
        buffer.print_buffer();

        buffer.close_buffer();

        return;
    }
}
