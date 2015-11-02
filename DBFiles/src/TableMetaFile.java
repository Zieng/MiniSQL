import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by zieng on 10/4/15.
 */
public class TableMetaFile {
    String tableName;
    String filename;
    ArrayList<AttributeMetaFile> colList = new ArrayList<AttributeMetaFile>();
    String primaryKey;
    int recordLen = 0;

    public boolean calculate_record_length()
    {
        recordLen=0;
        for(int i=0;i<this.colList.size();i++)
        {
            recordLen += colList.get(i).valueLen;
        }
        return true;
    }

    public boolean verify_attribute_value(ArrayList<String> valueList)
    {
        for(int j=0;j<colList.size();j++)
        {

        }
        return true;
    }

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
            System.out.println("\t\tlinearMetaFile::我都还没删除啊，怎得纪录文件就没了？");
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
