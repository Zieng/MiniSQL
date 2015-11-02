import java.util.ArrayList;

/**
 * Created by zieng on 10/14/15.
 */
public class TestCatalogManager
{
    public static void main(String [] args )
    {
        ArrayList<String> attributeList = new ArrayList<String>();
        ArrayList<String> typeList = new ArrayList<String>();
        ArrayList<String> uniqueList = new ArrayList<String>();

        attributeList.add("id");
        typeList.add("int");

        attributeList.add("name");
        typeList.add("char(12)");

        uniqueList.add("name");

        CatalogManager CM = new CatalogManager();

        CM.print_info();
        CM.insert_table_meta("student_2",attributeList,typeList,uniqueList,"id");
        CM.drop_table_meta("student_2");
        CM.print_info();
        CM.close_catalog_manager();
    }
}
