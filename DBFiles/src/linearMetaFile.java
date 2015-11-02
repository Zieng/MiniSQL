import java.io.File;
import java.io.IOException;

/**
 * Created by zieng on 10/4/15.
 */
public class linearMetaFile
{
    String tableName;
    String filename;     // filename=tableName.attribute.index
    String attribute;  // the index is built on this attribute
    int valueLen;  // 定义index文件中格式为：value+block_num+record_num,长度分别是：valueLen，4，4

    public void delete_file()
    {
        File f = new File(filename);
        if(f.exists())
        {
            f.delete();
            System.out.println("\t\tlinearMetaFile::成功清理残余文件"+filename);
        }
        else
        {
            System.out.println("\t\tlinearMetaFile::我都还没删除啊，怎么索引文件就没了？");
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
