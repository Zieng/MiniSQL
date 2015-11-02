import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by zieng on 10/25/15.
 */
public class IndexMetaFile
{
    String indexName;
    String tableName;
    String attribute;
    String filename;
    int valueLength;
    int rootBlock;
    ArrayList<Integer> existBlock;

    public void delete_file()
    {
        File f = new File(filename);
        if(f.exists())
        {
            f.delete();
            System.out.println("\t\tIndexMetaFile::成功清理残余文件"+filename);
        }
        else
        {
            System.out.println("\t\tIndexMetaFile::我都还没删除啊，怎么索引文件就没了？");
        }
    }

    public void create_file()
    {
        File f = new File(filename);

        if(f.exists())
            f.delete();

        try
        {
            if(!f.exists())
                f.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
