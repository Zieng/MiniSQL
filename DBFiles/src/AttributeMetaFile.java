/**
 * Created by zieng on 10/4/15.
 */
public class AttributeMetaFile
{
    String attributeName;
    String type;
    boolean unique;
    int valueLen;   // 字段长度

    public boolean type_value_verification(String value)
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
}
