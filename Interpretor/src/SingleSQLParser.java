import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

/**
 * Created by zieng on 9/28/15.
 */
public class SingleSQLParser {
//    ArrayList<String> sqlSegment;

    public static void main(String [] args){

    }

    // make sure the input of this method is a valid sql sentence
    public static boolean get_sql_string(String sql){
        sql=sql.replaceAll(";","");   // remove end ;
        sql=sql.trim();  // remove leading and trailing whitespaces
        sql=sql.toLowerCase();  // lower case
        sql=sql.replaceAll("\\s{1,}", " ");  // remove consecutive whitespaces to 1 whitespace

        System.out.println("\t执行："+sql+"\n");   // check
        if(sql.indexOf("insert into")!=-1){
            // this sql is insert command
            return parse_insert(sql);
        }
        else if (sql.indexOf("select")!=-1){
            // this sql is select command
            return parse_select(sql);
        }
        else if (sql.indexOf("delete from")!=-1){
            // this sql is delete command
            return parse_delete(sql);
        }
        else if(sql.indexOf("drop table")!=-1){
            // this sql is drop table command
            return parse_drop_table(sql);
        }
        else if (sql.indexOf("create table")!=-1){
            // this sql is create   table command
            return parse_create_table(sql);
        }
        else if (sql.indexOf("create index ")!=-1){
            return parse_create_index(sql);
        }
        else if(sql.indexOf("drop index")!=-1){
            return parse_drop_index(sql);
        }
        else
        {
            System.out.println("Invalid command\nFor more help, enter help");
        }

        return false;
    }

    public static boolean parse_select(String sql)
    {
        if(sql.indexOf(" from ")==-1 || sql.indexOf("select from")!=-1)
        {
            System.out.println("Invalid command! \nUsage: select attributes from tables [where conditions];");
            return false;
        }

        ArrayList<String> temp = new ArrayList(Arrays.asList( sql.split("(select)|(from)|(where)|(;)") ));
        ArrayList<String> sqlSegment= new ArrayList<String>();

        for (int i=0;i<temp.size();i++)
        {
            String word = temp.get(i);
            if(word.equals("") || word.equals(";") || word.equals(" "))
                continue;
            sqlSegment.add(word);
        }

        if(sqlSegment.size()==2 || sqlSegment.size()==3)
        {
            String attributeStr=sqlSegment.get(0).trim();
            String tableStr=sqlSegment.get(1).trim();
            String conditionStr="None";
            if (sqlSegment.size()==3)
                conditionStr=sqlSegment.get(2).trim();

//            System.out.println("SingleSQLParser::parse_select-->Test:");
//            System.out.println("\tTable to select="+tableStr);
//            System.out.println("\tAttributes="+attributeStr);
//            System.out.println("\tConditions="+conditionStr);

            return API_Manager.select_record(tableStr,attributeStr,conditionStr);
        }
        else
        {
            System.out.println("Invalid size! \n" +
                    "Usage: select attributes from tables [where conditions];");
            return false;
        }

    }

    public static boolean parse_create_table(String sql)
    {
        sql=sql.replaceAll("create table ","");
        ArrayList<String> temp = new ArrayList(Arrays.asList( sql.split("(\\()|(\\))") ));
        ArrayList<String> attrDefinition = new ArrayList<String>();
        if(temp.size()==0)
        {
            System.out.println("Invalid command! \nUsage: create table tablename (attribute1 constraint1,....,attributeN constraintN);");
            return false;
        }

        String tableName=temp.get(0).trim();
        sql=sql.replaceAll(tableName,"").trim();
        sql=sql.substring(1,sql.length());   // remove first '('
        sql=sql.substring(0,sql.length()-1);    // remove last ')'

        temp = new ArrayList(Arrays.asList( sql.split(",") ));
        for (int i=0;i<temp.size();i++)
        {
            String word = temp.get(i).trim();
            if(word.equals("") || word.equals("(") || word.equals(")"))
                continue;
            attrDefinition.add(word);
        }

//        System.out.println("----------------Test----------------");
//        System.out.println("create table "+tableName);
//
//        for(String x: attrDefinition)
//        {
//            System.out.println(x);
//        }
//        System.out.println("------------------------------------");

        // pass the tableName , the attributes list to function to create a table
        // To-do
        return API_Manager.create_table(tableName,attrDefinition);
    }


    public static boolean parse_create_index(String sql)
    {
        if(sql.indexOf("on")==-1 )
        {
            System.out.println("Invalid command! \nUsage: create index indexName on tables (attribute);");
            return false;
        }
        sql=sql.replaceAll("create index ","");
        ArrayList<String> temp = new ArrayList(Arrays.asList( sql.split("(on)|(\\()|(\\))|(;)") ));
        ArrayList<String> sqlSegment= new ArrayList<String>();

        for (int i=0;i<temp.size();i++)
        {
            String word = temp.get(i);
            if(word.equals("") || word.equals(" "))
                continue;
            sqlSegment.add(word);
        }

        if(sqlSegment.size()==3)
        {
            String indexStr=sqlSegment.get(0).trim();
            String tableStr=sqlSegment.get(1).trim();
            String attributeStr=sqlSegment.get(2).trim();


//            System.out.println("Test:");
//            System.out.println("\tTable to operate="+tableStr);
//            System.out.println("\tindex name="+indexStr);
//            System.out.println("\tattribure="+attributeStr);

            return API_Manager.create_index(tableStr,attributeStr,indexStr);
        }
        else
        {
            System.out.println("Invalid size! \n" +
                    "Usage: create index indexname on tablename (attribute) ;");
            return false;
        }
    }

    public static boolean parse_drop_table(String sql)
    {
        sql=sql.replaceAll("drop table ","");
        sql=sql.replaceAll(";","");
        sql=sql.trim();
        String tableName = sql;

        if(sql.equals(""))
        {
            System.out.println("Invalid size! \n" +
                    "Usage: drop table tablename ;");
            return false;
        }

        //to do
        return API_Manager.drop_table(tableName);
    }

    public static boolean parse_drop_index(String sql)
    {
        sql=sql.replaceAll("drop index ","");
        sql=sql.replaceAll(";","");
        sql=sql.trim();

        if(sql.equals("")==false)
        {
            System.out.println("Test: index to delete="+sql);
        }
        else
        {
            System.out.println("Invalid size! \n" +
                    "Usage: drop index indexname ;");
            return false;
        }
        System.out.println("going to drop index "+sql);

        return API_Manager.drop_index(sql);
    }

    public static boolean parse_insert(String sql)
    {
        if(sql.indexOf("values")==-1)
        {
            System.out.println("Invalid command! \nUsage: insert into values (value1,value2,...,valueN);");
            return false;
        }
        sql=sql.replaceAll("insert into ","");
        ArrayList<String> temp = new ArrayList(Arrays.asList( sql.split("(values)|(\\()|(\\))") ));
        ArrayList<String> sqlSegment= new ArrayList<String>();

        for (int i=0;i<temp.size();i++)
        {
            String word = temp.get(i);
            if(word.equals("") || word.equals(" "))
                continue;
            sqlSegment.add(word);
        }
//        System.out.println(sqlSegment.size());
//        for(String x:sqlSegment){
//            System.out.println(x);
//        }
        if(sqlSegment.size()==2)
        {
            String tableName=sqlSegment.get(0).trim();
            String valueStr=sqlSegment.get(1).trim();

            System.out.println("SingleSQLParser::parse_insert-->Test:");
            System.out.println("\tTable to insert="+tableName);
            System.out.println("\tValues string="+valueStr);
            valueStr=valueStr.trim();

            return API_Manager.insert_record(tableName,new ArrayList<String>(Arrays.asList(valueStr.split(","))));
        }
        else
        {
            System.out.println("Invalid size! \n" +
                    "Usage: insert into values (value1,value2,...,valueN)");
            return false;
        }

    }

    public static boolean parse_delete(String sql)
    {
        sql=sql.replaceAll("delete from ","");
        ArrayList<String> temp = new ArrayList(Arrays.asList( sql.split("where") ));
        ArrayList<String> sqlSegment= new ArrayList<String>();

        for (int i=0;i<temp.size();i++)
        {
            String word = temp.get(i);
            if(word.equals("") || word.equals(" "))
                continue;
            sqlSegment.add(word);
        }

        if(sqlSegment.size()==1 || sqlSegment.size()==2)
        {
            String tableStr=sqlSegment.get(0).trim();
            String conditionStr="None";
            if(sqlSegment.size()==2)
                conditionStr=sqlSegment.get(1).trim();

            System.out.println("Test:");
            System.out.println("\tTable to operate="+tableStr);
            System.out.println("\tcondition="+conditionStr);

            return API_Manager.delete_record(tableStr,conditionStr);
        }
        else
        {
            System.out.println("Invalid size! \n" +
                    "Usage: create index indexname on tablename (attribute) ;");
            return false;
        }
    }

    public static ArrayList<String> parse_attribue(String attributeStr)
    {
        // 只需要实现 select * from ....
        return new ArrayList<String>();
    }
}
