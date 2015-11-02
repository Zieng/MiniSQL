import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;

/**
 * Created by zieng on 10/4/15.
 */

public class CatalogManager
{
    static ArrayList<TableMetaFile> tableMetaFileList = new ArrayList<TableMetaFile>();
    static ArrayList<linearMetaFile> linearOrderMetaList = new ArrayList<linearMetaFile>();   // when there are no index,do search based on linearMetaFile
    static ArrayList<IndexMetaFile> indexMetaFileList = new ArrayList<IndexMetaFile>();

    public CatalogManager()
    {
        Gson gson = new Gson();

        // Load table Meta File...
        try
        {
            ArrayList<TableMetaFile> temp;
//            System.out.println("Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader("table_meta.json"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<TableMetaFile>>(){}.getType());
            if(temp==null)
            {
                System.out.println("关系元数据加载失败:(.....启用默认初始化设置");
            }
            else
            {
//                System.out.println("表元数据初始化成功");
                tableMetaFileList=temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // Load linear order meta file
        try
        {
            ArrayList<linearMetaFile> temp;
            BufferedReader br = new BufferedReader(
                    new FileReader("linear_meta.json"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<linearMetaFile>>(){}.getType());
            if(temp==null)
            {
                System.out.println("线性顺序文件元数据加载失败:(.....启用默认初始化设置");
            }
            else
            {
//                System.out.println("索引元数据初始化成功");
                linearOrderMetaList = temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // load index meta file
        try
        {
            ArrayList<IndexMetaFile> temp;
            BufferedReader br = new BufferedReader(
                    new FileReader("index_meta.json"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<IndexMetaFile>>(){}.getType());
            if(temp==null)
            {
                System.out.println("索引元数据加载失败:(.....启用默认初始化设置");
            }
            else
            {
//                System.out.println("索引元数据初始化成功");
                indexMetaFileList = temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static TableMetaFile get_table_meta(String tableName)
    {
        for(int i=0;i<tableMetaFileList.size();i++)
        {
            if(tableMetaFileList.get(i).tableName.equals(tableName))
                return tableMetaFileList.get(i);
        }
        return null;
    }

    public static linearMetaFile get_linear_meta(String tableName, String attribute)
    {
        for(int i=0;i< linearOrderMetaList.size();i++)
        {
            if(linearOrderMetaList.get(i).tableName.equals(tableName) && linearOrderMetaList.get(i).attribute.equals(attribute))
                return linearOrderMetaList.get(i);
        }
        return null;
    }

    public static int get_attribute_size(String tableName,String attribute)
    {
//        System.out.println("\tIn catalog manager:get_attribute size for "+tableName+","+attribute);
        for(int i=0;i<tableMetaFileList.size();i++)
        {
            TableMetaFile tmf = tableMetaFileList.get(i);
            if(tmf.tableName.equals(tableName))
            {
//                System.out.println("match tablename "+tableName);
                for(int j=0;j<tmf.colList.size();j++)
                {
                    if(tmf.colList.get(j).attributeName.equals(attribute))
                        return tmf.colList.get(j).valueLen;
                }
            }
        }
        return -1;
    }

    public static boolean check_table_exist(String tableName)
    {
        for(int i=0;i<tableMetaFileList.size();i++)
        {
            if(tableMetaFileList.get(i).tableName.equals(tableName))
                return true;
        }
        return false;
    }

    public static boolean check_attribute_exist(String tableName,String attribute)
    {
        for(int i=0;i<tableMetaFileList.size();i++)
        {
            TableMetaFile tmf = tableMetaFileList.get(i);
            if (tmf.tableName.equals(tableName))
            {
                for(int j=0;j<tmf.colList.size();j++)
                {
                    AttributeMetaFile amf = tmf.colList.get(j);
                    if(amf.attributeName.equals(attribute))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean insert_linear_meta(String tableName, String attribute)
    {
        linearMetaFile imf = new linearMetaFile();
        imf.tableName=tableName;
        imf.filename= tableName+"."+attribute+".linear";
        imf.attribute=attribute;
        imf.valueLen = get_attribute_size(tableName,attribute);
        if(imf.valueLen <=0)
        {
            System.out.println("CatalogManager::insert_linear_meta-->可恶的索引太难搞了，程序罢工了！");
            return false;
        }
        if(linearOrderMetaList.contains(imf))
        {
            System.out.println("CatalogManager::insert_linear_meta-->The index is already exist!");
            return false;
        }
        imf.create_file();
        linearOrderMetaList.add(imf);

        // create a correspond index file to store record
//        BufferManager bf = new BufferManager(1);
//        BufferManager.get_block_readwrite(imf.filename,0);
//        BufferManager.print_buffer();
//        BufferManager.return_block(imf.filename,0,true);
//        BufferManager.print_buffer();
//        BufferManager.close_buffer();

        return true;
    }

    public static boolean drop_linear_meta(String tableName, String attribute)
    {
        Iterator<linearMetaFile> it = linearOrderMetaList.iterator();
        while(it.hasNext())
        {
            linearMetaFile x = it.next();
            if(x.tableName.equals(tableName) && x.attribute.equals(attribute))
            {
                x.delete_file();
                it.remove();
                System.out.println("The index is successfully dropped");
                return true;
            }
        }
        System.out.println("No correspond index found!");
        return false;
    }

    public static boolean drop_linear_meta(String tableName)
    {
        Iterator<linearMetaFile> index = linearOrderMetaList.iterator();
        while(index.hasNext())
        {
            linearMetaFile imf = index.next();
            if(imf.tableName.equals(tableName))
            {
                imf.delete_file();
                index.remove();
            }
        }
        return true;
    }

    public static boolean insert_table_meta(String tableName,ArrayList<String> attributeList,ArrayList<String> typeList,ArrayList<String> uniqueList,String pk)
    {
        TableMetaFile tmf = new TableMetaFile();

        if(attributeList.size()!= typeList.size())
        {
            System.out.println("CatalogManager::insert_table_meta-->Some attributes' type is wrong");
            return false;
        }

        for(int i=0;i<tableMetaFileList.size();i++)
        {
            if(tableMetaFileList.get(i).tableName.equals(tableName))
            {
                System.out.println("CatalogManager::insert_table_meta-->The table "+tableName+" is already exist!");
                return false;
            }
        }
        tmf.tableName=tableName;
        tmf.filename=tableName+".dbfile";
        tmf.primaryKey=pk;
        for(int i=0;i<attributeList.size();i++)
        {
            AttributeMetaFile amf = new AttributeMetaFile();
            amf.attributeName=attributeList.get(i);
            amf.type=typeList.get(i);
            amf.valueLen=length_of_type(amf.type);
            if(amf.valueLen <=0 )
            {
                return false;
            }
            if(uniqueList.indexOf( amf.attributeName )!=-1)
                amf.unique=true;
            else
                amf.unique=false;
            tmf.colList.add(amf);
        }
        tmf.calculate_record_length();
        tmf.create_file();               //create table-record file
        tableMetaFileList.add(tmf);

        // create a correspond table file to store record
        BufferManager bf = new BufferManager(1);
        BufferManager.get_block_readwrite(tmf.filename,0);
//        BufferManager.print_buffer();
        BufferManager.return_block(tmf.filename,0,true);
//        BufferManager.print_buffer();
        BufferManager.close_buffer();

        return true;
    }

    public static boolean drop_table_meta(String tableName)
    {
        Iterator<TableMetaFile> it = tableMetaFileList.iterator();
        while(it.hasNext())
        {
            TableMetaFile x = it.next();
            if(x.tableName.equals(tableName))
            {
                x.delete_file();     // delete table-record file
                it.remove();
                // 删除所有建在此表上的索引
                drop_linear_meta(tableName);
                drop_index(tableName,0);
//                try
//                {
//                    File f = new File(x.filename);
//                    if(!f.delete())
//                        System.out.println("CatalogManager::drop_table_meta-->清除表的数据文件时失败，请手动删除"+x.filename+"文件！");
//                }
//                catch (Exception e)
//                {
//                    e.printStackTrace();
//                }
//                System.out.println("CatalogManager::drop_table_meta-->Table "+tableName+" is successfully dropped!");
                return true;
            }
        }
        System.out.println("CatalogManager::drop_table_meta-->No table named "+tableName+" found!");
        return false;
    }

    public static boolean type_value_verification(String type, String value)
    {
        if(type.matches("char\\([0-9]+\\)"))
        {
            type=type.replaceAll("char\\(","");
            type=type.replaceAll("\\)","");
            int length= Integer.parseInt(type);
            if(value.length()>length)
            {
                System.out.println("length overflow:>"+length);
                return false;
            }
            return true;
        }
        else if(type.equals("int") && value.matches("^[-]*[0-9]+$"))
        {
            System.out.println("Test:the "+value+" is int");
            return true;
        }
        else if(type.equals("float") && value.matches("^[-]*[0-9]+[.]*[0-9]*$"))
        {
            System.out.println("Test:the " + value + " is float");
            return true;
        }

        return false;
    }


    public static int length_of_type(String type)
    {
        if(type.equals("int"))
        {
            return 4;
        }
        else if(type.equals("float"))
        {
            return 4;
        }
        else if(type.matches("char\\([0-9]+\\)"))
        {
            type=type.replaceAll("char\\(","");
            type=type.replaceAll("\\)","");
            return Integer.parseInt(type);
        }
        else
        {
            System.out.println("Invalid type: "+type);
            return -1;
        }
    }



    //for index manager function
    public static boolean check_index_exist(String indexName)
    {
//        System.out.println( indexMetaFileList.size() );

        for(IndexMetaFile imf:indexMetaFileList)
        {
//            System.out.println("NullPointer?");
//            System.out.println("check imf is NULL:");
//            System.out.println(imf==null);
//            System.out.println("check imf.indexName is NULL:");
//            System.out.println(imf.indexName==null);
            if(imf.indexName.equals(indexName))
                return true;
        }

        return false;
    }

    public static boolean check_index_exist(String tableName,String attribute)
    {
        for(IndexMetaFile imf:indexMetaFileList)
        {
            if(imf.tableName.equals(tableName) && imf.attribute.equals(attribute))
            {
//                System.out.println(tableName+","+attribute+"存在索引");
                return true;
            }
        }
        return false;
    }

    public static boolean create_index(String indexName,String tableName,String attribute)
    {
        if(check_index_exist(indexName) || check_index_exist(tableName,attribute))
        {
            System.out.println("CatalogManager::create_index---->明明都已经有这个索引了，就别再重复创建了吧。。。");
            return false;
        }
        IndexMetaFile imf = new IndexMetaFile();
        TableMetaFile tmf = get_table_meta(tableName);
        if(tmf==null)
        {
            System.out.println("CatalogManager::create_index--->绝对是API_Manager忘了检查table是否存在！");
            return false;
        }

        imf.tableName=tableName;
        imf.attribute=attribute;
        imf.indexName=indexName;
        imf.filename=indexName+".index";
        imf.rootBlock=0;
        imf.valueLength=-1;
        imf.existBlock = new ArrayList<Integer>();

        for(AttributeMetaFile amf:tmf.colList)
        {
            if(amf.attributeName.equals(attribute))
                imf.valueLength=amf.valueLen;
        }
        if(imf.valueLength==-1)
        {
            System.out.println("CatalogManager::create_index--->绝对是API_Manager忘了检查attribute是否存在！");
            return false;
        }
        imf.create_file();

        indexMetaFileList.add(imf);

        return true;
    }

    public static boolean update_index(String indexName,int newRootBlock, ArrayList<Integer> blockSet)
    {
        IndexMetaFile imf = get_index_meta(indexName);
        if(imf==null)
            return false;
        imf.rootBlock=newRootBlock;
        imf.existBlock = new ArrayList<Integer>();

        Integer x ;
        for(int i=0;i<blockSet.size();i++)
        {
            x = new Integer(blockSet.get(i));
            imf.existBlock.add(x);
        }

        System.out.println("\t\tupdate index ok");

        return true;
    }

    public static boolean update_index(String tableName,String attribute,int newRootBlock,ArrayList<Integer> blockSet)
    {
        IndexMetaFile imf = get_index_meta(tableName,attribute);
        if(imf==null)
            return false;
        imf.rootBlock=newRootBlock;
        imf.existBlock = new ArrayList<Integer>();

        Integer x;
        for(int i=0;i<blockSet.size();i++)
        {
            x = new Integer(blockSet.get(i));
            imf.existBlock.add(x);
        }
        System.out.println("\t\tupdate index ok");

        return true;
    }

    public static boolean drop_index(String indexName)
    {
        Iterator<IndexMetaFile> it = indexMetaFileList.iterator();
        while(it.hasNext())
        {
            IndexMetaFile imf= it.next();
            if(imf.indexName.equals(indexName))
            {
                imf.delete_file();
                it.remove();
            }
        }

        return false;
    }

    public static boolean drop_index(String tableName,int mode)     // int mode is nothing but to differ from drop_index(indexName)
    {
        Iterator<IndexMetaFile> it = indexMetaFileList.iterator();
        while(it.hasNext())
        {
            IndexMetaFile imf= it.next();
            if(imf.tableName.equals(tableName))
            {
                imf.delete_file();
                it.remove();
            }
        }
        return false;
    }

    public static IndexMetaFile get_index_meta(String indexName)
    {
        for(IndexMetaFile imf:indexMetaFileList)
            if(imf.indexName.equals(indexName))
                return imf;

        return null;
    }

    public static IndexMetaFile get_index_meta(String tableName,String attribute)
    {
        for(IndexMetaFile imf:indexMetaFileList)
            if(imf.tableName.equals(tableName) && imf.attribute.equals(attribute))
                return imf;

        return null;
    }


    // close catalog manager
    public static void close_catalog_manager()
    {
        Gson gson = new Gson();

        // write back the table meta file
        String json = gson.toJson(tableMetaFileList);
        try
        {
            System.out.println("CatalogManager::close_catalog_manager-->write table meta data to file......");
            FileWriter writer = new FileWriter("table_meta.json");
            writer.write(json);
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // write back the linear order meta file
        json = gson.toJson(linearOrderMetaList);
        try
        {
            System.out.println("CatalogManager::close_catalog_manager-->write linear order meta data to file......");
            FileWriter writer = new FileWriter("linear_meta.json");
            writer.write(json);
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // write back the index meta file
        json = gson.toJson(indexMetaFileList);
        try
        {
            System.out.println("CatalogManager::close_catalog_manager-->write index meta data to file......");
            FileWriter writer = new FileWriter("index_meta.json");
            writer.write(json);
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //debug
    public static void print_info()
    {
        System.out.println("----------------------Table Meta Data---------------------------------");
        if(tableMetaFileList.size()==0)
            System.out.println("[-----Empty-----]");
        for(TableMetaFile tmf:tableMetaFileList)
        {
            System.out.println("\t[table name: "+tmf.tableName+"]");
            System.out.println("\tstore in: "+tmf.filename);
            System.out.println("\tprimary key: "+tmf.primaryKey);
            System.out.println("\trecord size: "+tmf.recordLen);
            for(AttributeMetaFile amf:tmf.colList)
            {
                System.out.println("\t\tattribute name: "+amf.attributeName);
                System.out.println("\t\tattribute type: "+amf.type);
                System.out.println("\t\tattribute size: "+amf.valueLen);
                System.out.println("\t\tunique: "+amf.unique);
                System.out.println("\t\t---------------------------------");
            }
            System.out.println();
        }

        System.out.println("----------------------Index Meta Data---------------------------------");
        if(indexMetaFileList.size()==0)
            System.out.println("[-----Empty-----]");
        for(IndexMetaFile imf: indexMetaFileList)
        {
            System.out.println("\t[IndexName: "+imf.indexName+"]");
            System.out.println("\tStored in: "+imf.filename);
            System.out.println("\tbased on : table="+imf.tableName+",attribute="+imf.attribute+"(length="+imf.valueLength+")");
            System.out.println("\tIndex root block:"+imf.rootBlock);
            System.out.println("\tall index block:"+imf.existBlock.toString());
            System.out.println();
        }
    }
}
