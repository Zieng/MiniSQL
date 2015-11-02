import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zieng on 10/14/15.
 */

public class API_Manager
{
    public API_Manager()
    {
        //set the buffer size
        BufferManager bf = new BufferManager(4);
        CatalogManager cf = new CatalogManager();  // load init data
    }

    public static void close_api()
    {
        BufferManager.close_buffer();
        CatalogManager.close_catalog_manager();
//        RecordManager.close_linear_search();
    }

    public static boolean create_table(String tableName,ArrayList<String> attrDefinition)
    {
        ArrayList<String> attributeList = new ArrayList<String>();
        ArrayList<String> typeList = new ArrayList<String>();
        ArrayList<String> uniqueList = new ArrayList<String>();
        String pk="null";   //primary key

        if(CatalogManager.check_table_exist(tableName))
        {
            System.out.println("API::create_table-->table "+tableName+" is already exist!");
            return false;
        }

        for(String x:attrDefinition)
        {
            if(x.contains("primary key"))
            {
                x=x.replaceAll("primary key","");
                x=x.replaceAll("\\(","");
                x=x.replaceAll("\\)","");
                pk=x;
                continue;
            }
            ArrayList<String> temp = new ArrayList<String>(Arrays.asList(x.split(" ")));
            if(temp.size()<2)
            {
                System.out.println("API::create_table-->invalid attribute definitions!");
                return false;
            }
            attributeList.add(temp.get(0));
            typeList.add(temp.get(1));
            if(x.contains("unique"))
            {
                uniqueList.add(temp.get(0));
            }
        }
        if(pk.equals("null"))
        {
            System.out.println("API::create_table-->没有定义主键你好意思建一个表？");
            return false;
        }

//        CatalogManager.print_info();
        CatalogManager.insert_table_meta(tableName,attributeList,typeList,uniqueList,pk);

        // create a index on pk by default
        CatalogManager.insert_linear_meta(tableName, pk);


        return true;
    }

    public static boolean drop_table(String tableName)
    {


        return CatalogManager.drop_table_meta(tableName);
    }

    public static boolean insert_record(String tableName,ArrayList<String> valueList)
    {
        if(CatalogManager.check_table_exist(tableName)==false)
        {
            System.out.println("The table "+ tableName+" is not existed");
            return false;
        }
        TableMetaFile tmf = CatalogManager.get_table_meta(tableName);
        if(tmf==null)
        {
            System.out.println("API::insert_record-->There isn't a table called "+tableName+" in the database! Please check your spell!");
            return false;
        }
        int attrNum = tmf.colList.size();
        if(valueList.size() % attrNum != 0)
        {
            System.out.println("API::insert_record-->The numbers of value you want to insert isn't match the record's size! ");
            System.out.println("API::insert_record-->use show + tableName to check the table's definition");
            return false;
        }
        int recordNum = valueList.size() / attrNum;
        for(int i=0;i<recordNum;i++)       // insert all the new record
        {
            ArrayList<String> valueList_per_record = new ArrayList<String>();
            String pkValue="主键失踪了";
            for(int j=0;j<attrNum;j++)      // 进行type-value匹配性检查
            {
                String v=valueList.get(i*attrNum+j).trim();
                AttributeMetaFile amf = tmf.colList.get(j);
                if(amf.type_value_verification(v)==false)
                {
                    System.out.println("API::insert_record-->The value "+v+" does not match the required type \'"+amf.type+"\' of "+amf.attributeName);
                    return false;
                }
                if(amf.attributeName.equals(tmf.primaryKey))
                    pkValue=v;
                valueList_per_record.add(v);
            }
            if(pkValue.equals("主键失踪了"))    // check the primary key
            {
                System.out.println("API::insert_record-->"+pkValue);
                return false;
            }

            LinearSearchUnit lsu = RecordManager.insert_record_linear(tableName, tmf.primaryKey, pkValue, tmf.recordLen);
            if(lsu==null)
            {
                System.out.println("API::insert_record-->没法插入线性顺序文件，所以你也没法插入数据");
                return false;
            }

            if(!RecordManager.insert_record(tableName,tmf.colList,valueList_per_record,lsu.blockNum,lsu.recordNum,tmf.recordLen))
            {
                System.out.println("API::insert_record-->插入数据时好像走神了");
                return false;
            }

            // update the index if index exist
            for(int j=0;j<tmf.colList.size();j++)
            {
                AttributeMetaFile amf = tmf.colList.get(j);

                if( CatalogManager.check_index_exist(tableName,amf.attributeName))
                {
                    IndexMetaFile imf = CatalogManager.get_index_meta(tableName,amf.attributeName);
                    if(imf==null)
                    {
                        System.out.println("API_manager::insert_record---->读取索引meta数据失败了");
                        return false;
                    }
                    IndexContext ic = new IndexContext();
                    ic.filename = imf.filename;
                    ic.rootBlock = imf.rootBlock;
                    ic.existBlock = imf.existBlock;
                    ic.keyType = amf.type;
                    ic.keyLength = amf.valueLen;

                    IndexManager.set_index_context(ic);
                    System.out.println("Going to insert into B+ tree:\tkey="+valueList_per_record.get(j)+"("+ic.keyType+"),block="+lsu.blockNum+",record="+lsu.recordNum);
                    System.out.println();
                    IndexManager.insert_index(valueList_per_record.get(j),lsu.blockNum,lsu.recordNum);
                    System.out.println();

                    ic = IndexManager.get_index_context();

                    CatalogManager.update_index(tableName,amf.attributeName,ic.rootBlock,ic.existBlock);
                }
            }
        }
        return true;
    }

    public static boolean select_record(String tableName,String attributes,String condition)
    {
        TableMetaFile tmf = CatalogManager.get_table_meta(tableName);
        if(tmf==null)
        {
            System.out.println("API::select_record-->Table "+tableName+" is not existed!");
            return false;
        }
        if(!attributes.equals("*"))
        {
            System.out.println("API::select_record-->我们家的数据库只支持select * from xxx [where ....]");
            return false;
        }

        if(condition.equals("None"))   // 没有[where ...]
        {
            int i;
            for(i=0;i<tmf.colList.size();i++)
            {
                AttributeMetaFile amf = tmf.colList.get(i);
                if( CatalogManager.check_index_exist(tableName,amf.attributeName))
                {
                    System.out.println("["+tableName+"存在索引，将通过索引来搜索]");
                    break;
                }
            }
            if(i<tmf.colList.size())   // means there exist index on some attributes, so should query depend on index
            {


                AttributeMetaFile amf = tmf.colList.get(i);
                IndexMetaFile imf = CatalogManager.get_index_meta(tableName,amf.attributeName);
                if(imf==null)
                {
                    System.out.println("API_manager::insert_record---->读取索引meta数据失败了");
                    return false;
                }
                IndexContext ic = new IndexContext();
                ic.filename = imf.filename;
                ic.rootBlock = imf.rootBlock;
                ic.existBlock = imf.existBlock;
                ic.keyType = amf.type;
                ic.keyLength = amf.valueLen;

                IndexManager.set_index_context(ic);
                IndexManager.find_all_index();
                ic = IndexManager.get_index_context();

                System.out.println("总共找到"+ic.resultBlockSet.size()+"条纪录，数据以"+amf.attributeName+"递增排列");
                System.out.println("------------------------------------------------------------------------");
                System.out.print("|");
                for(i=0;i<tmf.colList.size();i++)
                {
                    AttributeMetaFile tempAmf = tmf.colList.get(i);
                    System.out.print("\t"+tempAmf.attributeName+"\t|");
                }
                System.out.println();
                System.out.println("------------------------------------------------------------------------");

                for(int j=0;j<ic.resultBlockSet.size();j++)
                {
                    int blockNum = ic.resultBlockSet.get(j);
                    int recordNum = ic.resultRecordSet.get(j);

                    if(!RecordManager.select_record(tableName,tmf.colList,blockNum,recordNum,tmf.recordLen))
                    {
                        System.out.println("API::select_record-->RecordManager他不给我纪录数据");
                        return false;
                    }
                }
                System.out.println();
                System.out.println("------------------------------------------------------------------------");
            }
            else         // no index exist on such table ,only query based on linear order
            {
                ArrayList<LinearSearchUnit> indexList = RecordManager.get_record_position_linear(tableName,tmf.primaryKey);
                if(indexList==null)
                {
                    System.out.println("API::select_record-->API 拿不到索引，只能先撤了。。。。");
                    return false;
                }
                System.out.println("总共找到"+indexList.size()+"条纪录");
                System.out.println("------------------------------------------------------------------------");
                System.out.print("|");
                for(i=0;i<tmf.colList.size();i++)
                {
                    AttributeMetaFile amf = tmf.colList.get(i);
                    System.out.print("\t"+amf.attributeName+"\t|");
                }
                System.out.println();
                System.out.println("------------------------------------------------------------------------");

                for(i=0;i<indexList.size();i++)
                {
                    LinearSearchUnit iu = indexList.get(i);
                    if(!RecordManager.select_record(tableName,tmf.colList,iu.blockNum,iu.recordNum,tmf.recordLen))
                    {
                        System.out.println("API::select_record-->RecordManager他不给我纪录数据");
                        return false;
                    }

                }
                RecordManager.close_linear_search();
            }
        }
        else
        {
//            System.out.println("API::select_record-->还没想好要怎么处理带条件的index查询，所以只能走线性查询了");

            ArrayList<Integer> resultBlockSet = new ArrayList<Integer>();
            ArrayList<Integer> resultRecordSet = new ArrayList<Integer>();

            ArrayList<String> conditionList = new ArrayList<String>(Arrays.asList(condition.split("and")));
            ArrayList<String> attrNameList = new ArrayList<String>();

            for(int i=0;i<tmf.colList.size();i++)
            {
                attrNameList.add(new String(tmf.colList.get(i).attributeName));
            }

            boolean firstFilter = true;
            Iterator iter = conditionList.iterator();

            while(iter.hasNext())        // filter based on index
            {
                String cond = (String) iter.next();
                cond = cond.trim();

                String op,Attr,Value,separator="";
                ArrayList<String> tokens;

                if(cond.contains(">="))
                    separator=">=";
                else if(cond.contains("<="))
                    separator="<=";
                else if(cond.contains("!="))
                    separator="!=";
                else if(cond.contains(">"))
                    separator=">";
                else if(cond.contains("<"))
                    separator="<";
                else if(cond.contains("="))
                    separator="=";

                tokens = new ArrayList<String>(Arrays.asList(cond.split(separator)));

                if(tokens.size()!=2)
                {
                    System.out.println("\t\tAPI::select_record-->哎呀，你的where条件语句没写对");
                    return false;
                }
                Attr=tokens.get(0).trim();
                if(attrNameList.contains(Attr))   // default condition format: [attribute + operation + value]
                {
                    Value = tokens.get(1).trim();
                    op = separator;
                }
                else                                    // the format is [value + operation + attribute]
                {
                    Value= tokens.get(0).trim();
                    Attr= tokens.get(1).trim();
                    // reverse the operator
                    if(separator.equals(">="))
                        op="<=";
                    else if(separator.equals("<="))
                        op=">=";
                    else if(separator.equals("<"))
                        op=">";
                    else if(separator.equals(">"))
                        op="<";
                    else
                        op=separator;
                }

                int index = attrNameList.indexOf(Attr);
                if(index <0 || index>=tmf.colList.size())
                {
                    System.out.println(Attr+"对应的index＝"+index);
                    System.out.println("\t\tAPI::select_record-->表"+tableName+"里貌似没有"+Attr+"这个属性吧？");
                    return false;
                }

                IndexMetaFile imf = CatalogManager.get_index_meta(tableName,Attr);
                if( imf != null )            //    if attribute has index
                {
                    System.out.println("\t\tAPI::select_record--->超级快！通过"+Attr+"上的索引"+imf.indexName+"来进行过滤");

                    AttributeMetaFile amf = tmf.colList.get(index);
                    IndexContext ic = new IndexContext();
                    ic.filename = imf.filename;
                    ic.rootBlock = imf.rootBlock;
                    ic.existBlock = imf.existBlock;
                    ic.keyType = amf.type;
                    ic.keyLength = amf.valueLen;

                    IndexManager.set_index_context(ic);

                    IndexManager.find_index_condition(op,Value);

                    ic = IndexManager.get_index_context();

                    ArrayList tempB = new ArrayList<Integer>( ic.resultBlockSet );
                    ArrayList tempR = new ArrayList<Integer>( ic.resultRecordSet);

                    if(firstFilter)
                    {
                        resultBlockSet = tempB;
                        resultRecordSet = tempR;
//                        System.out.println("resultBlockSet.size="+resultBlockSet.size());
                        firstFilter = false;
                    }
                    else              // intersection
                    {
                        resultBlockSet.retainAll(tempB);
                        resultRecordSet.retainAll(tempR);
                    }

                    if(resultBlockSet.size() <= 1024)   //   small enough, no need further filter
                        break;
                    else
                        System.out.println("\t\tAPI::select_record--->继续通过" + Attr + "上的索引" + imf.indexName + "来进行过滤");

                    iter.remove();
                }

            }

            if( resultBlockSet.size() != 0)  // means there are some index filter
            {
                System.out.println("经过宇宙无敌超级B+树过滤了之后还有"+resultBlockSet.size()+"条纪录，其中符合查询条件的如下：");
                System.out.println("------------------------------------------------------------------------");
                System.out.print("|");
                for(int i=0;i<tmf.colList.size();i++)
                {
                    AttributeMetaFile amf = tmf.colList.get(i);
                    System.out.print("\t" + amf.attributeName + "\t|");
                }
                System.out.println();
                System.out.println("------------------------------------------------------------------------");

                for(int i=0;i<resultBlockSet.size();i++)
                {
                    //todo 还能进一步优化
                    if(!RecordManager.select_record_condition(tableName, tmf.colList, condition, resultBlockSet.get(i), resultRecordSet.get(i), tmf.recordLen))
                    {
                        System.out.println("\t\tAPI::select_record-->RecordManager他不给我纪录数据");
                        return false;
                    }
                }
                System.out.println("------------------------------------------------------------------------");
            }
            else         // no index
            {
                ArrayList<LinearSearchUnit> lsuList = RecordManager.get_record_position_linear(tableName,tmf.primaryKey);
                if(lsuList==null)
                {
                    System.out.println("API::select_record-->API 拿不到文件记录表，只能先撤了。。。。");
                    return false;
                }

                System.out.println("［－－where语句中出现的属性没有索引可以优化查找－－］");
                System.out.println("总共找到" + lsuList.size() + "条纪录，其中符合查询条件的如下：");
                System.out.println("------------------------------------------------------------------------");
                System.out.print("|");
                for(int i=0;i<tmf.colList.size();i++)
                {
                    AttributeMetaFile amf = tmf.colList.get(i);
                    System.out.print("\t" + amf.attributeName + "\t|");
                }
                System.out.println();
                System.out.println("------------------------------------------------------------------------");

                for(int i=0;i<lsuList.size();i++)
                {
                    LinearSearchUnit lsu = lsuList.get(i);
                    if(!RecordManager.select_record_condition(tableName, tmf.colList, condition, lsu.blockNum, lsu.recordNum, tmf.recordLen))
                    {
                        System.out.println("\t\tAPI::select_record-->RecordManager他不给我纪录数据");
                        return false;
                    }
                }
                RecordManager.close_linear_search();
            }

        }

        return true;
    }

    public static boolean delete_record(String tableName,String condition)
    {
        TableMetaFile tmf = CatalogManager.get_table_meta(tableName);
        if(tmf==null)
        {
            System.out.println("API::select_record-->Table "+tableName+" is not existed!");
            return false;
        }

        if(condition.equals("None"))
        {
            RecordManager.delete_all_linear_record(tableName, tmf.primaryKey);

            // update the index if index exist
            for(int j=0;j<tmf.colList.size();j++)
            {
                AttributeMetaFile amf = tmf.colList.get(j);

                if( CatalogManager.check_index_exist(tableName,amf.attributeName))
                {
                    IndexMetaFile imf = CatalogManager.get_index_meta(tableName,amf.attributeName);
                    if(imf==null)
                    {
                        System.out.println("\t\tAPI_manager::insert_record---->读取索引meta数据失败了");
                        return false;
                    }
                    IndexContext ic = new IndexContext();
                    ic.filename = imf.filename;
                    ic.rootBlock = imf.rootBlock;
                    ic.existBlock = imf.existBlock;
                    ic.keyType = amf.type;
                    ic.keyLength = amf.valueLen;

                    IndexManager.set_index_context(ic);

                    IndexManager.purge_index();

                    CatalogManager.update_index(tableName,amf.attributeName,ic.rootBlock,ic.existBlock);
                }
            }
        }
        else
        {
            ArrayList<LinearSearchUnit> lsuList = RecordManager.get_record_position_linear(tableName,tmf.primaryKey);
            if(lsuList==null)
            {
                System.out.println("API::select_record-->API 拿不到文件记录表，只能先撤了。。。。");
                return false;
            }
            RecordManager.close_linear_search();

            System.out.println("总共找到" + lsuList.size() + "条纪录，将对其进行选择删除：");

            for(int i=0;i<lsuList.size();i++)
            {
                ArrayList<String> valueList = new ArrayList<String>();

                LinearSearchUnit lsu = lsuList.get(i);
                valueList = RecordManager.delete_record_condition(tableName,tmf.primaryKey,tmf.colList, condition, lsu.blockNum, lsu.recordNum, tmf.recordLen);
                if(valueList == null)
                {
//                    System.out.println("\t\tAPI::select_record-->RecordManager他不告诉我它删除了哪些纪录");
                    continue;
                }

                System.out.println("ValueList of deleted record:"+valueList.toString());
                if(valueList.size()==0)
                    continue;

                //todo
                // update the index if index exist
                for(int j=0;j<tmf.colList.size();j++)
                {
                    AttributeMetaFile amf = tmf.colList.get(j);

                    if( CatalogManager.check_index_exist(tableName,amf.attributeName))
                    {
                        IndexMetaFile imf = CatalogManager.get_index_meta(tableName,amf.attributeName);
                        if(imf==null)
                        {
                            System.out.println("API_manager::insert_record---->读取索引meta数据失败了");
                            return false;
                        }
                        System.out.println("\t\tAPI::delete_record--->存在索引"+imf.indexName+",正在对其进行更新.....");
                        IndexContext ic = new IndexContext();
                        ic.filename = imf.filename;
                        ic.rootBlock = imf.rootBlock;
                        ic.existBlock = imf.existBlock;
                        ic.keyType = amf.type;
                        ic.keyLength = amf.valueLen;

                        IndexManager.set_index_context(ic);
                        System.out.println("Going to delete from B+ tree:\tkey="+lsu.value+",block="+lsu.blockNum+",record="+lsu.recordNum);
                        System.out.println();
                        IndexManager.delete_index(valueList.get(j), lsu.blockNum, lsu.recordNum);
                        System.out.println();

                        ic = IndexManager.get_index_context();

                        CatalogManager.update_index(tableName,amf.attributeName,ic.rootBlock,ic.existBlock);
                    }
                }
            }

            return false;
        }
        return true;
    }

    public static boolean create_index(String tableName,String attribute,String indexName)
    {
        if( CatalogManager.check_index_exist(indexName) || CatalogManager.check_index_exist(tableName,attribute))
        {
            System.out.println("API::create_index---->别逗了，你已经创建过此索引了");
            return false;
        }

        if( ! CatalogManager.create_index(indexName,tableName,attribute))
        {
            System.out.println("API::create_index----->不好意思，创建索引失败");
            return false;
        }

        // update the index from linear
        TableMetaFile tmf = CatalogManager.get_table_meta(tableName);
        if(tmf==null)
        {
            System.out.println("API::select_record-->Table "+tableName+" is not existed!");
            return false;
        }
        AttributeMetaFile amf = new AttributeMetaFile();
        AttributeMetaFile tempAmf = new AttributeMetaFile();
        for(int i=0;i<tmf.colList.size();i++)
        {
            tempAmf = tmf.colList.get(i);
            if(tempAmf.attributeName.equals(attribute))
            {
                amf = tempAmf;
                break;
            }
        }
        if(amf == null)
        {
            System.out.println("API::create_index---->can't fetch the attribute info");
            return false;
        }
        ArrayList<LinearSearchUnit> lsuList = RecordManager.get_record_position_linear(tableName,tmf.primaryKey);
        if(lsuList==null)
        {
            System.out.println("API::create_index-->API 拿不到索引，只能先撤了。。。。");
            return false;
        }

        // prepare the index manager
        IndexMetaFile imf = CatalogManager.get_index_meta(tableName,amf.attributeName);
        if(imf==null)
        {
            System.out.println("API_manager::insert_record---->读取索引meta数据失败了");
            return false;
        }
        IndexContext ic = new IndexContext();
        ic.filename = imf.filename;
        ic.rootBlock = imf.rootBlock;
        ic.existBlock = new ArrayList<Integer>(imf.existBlock);
        ic.keyType = amf.type;
        ic.keyLength = amf.valueLen;

        IndexManager.set_index_context(ic);

        LinearSearchUnit iu;
        for(int i=0;i<lsuList.size();i++)
        {
            iu = lsuList.get(i);
            System.out.println("Going to insert into B+ tree:\tkey="+iu.value+",block="+iu.blockNum+",record="+iu.recordNum);
            System.out.println();
            IndexManager.insert_index(iu.value,iu.blockNum,iu.recordNum);
            System.out.println();
        }
        ic = IndexManager.get_index_context();

        // update index meta data
        CatalogManager.update_index(indexName,ic.rootBlock,ic.existBlock);

        return true;
    }


    public static boolean drop_index(String indexName)
    {
        return CatalogManager.drop_index(indexName);
    }




    // value check functions
    public static boolean type_value_verification(String type,String value)
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

    public static boolean check_condition_match(AttributeMetaFile amf, String attrValue, String op,String value)
    {
        if(!type_value_verification(amf.type,value))
            return false;

        if(amf.type.equals("int"))
        {
            Integer op1= Integer.parseInt(attrValue);
            Integer op2= Integer.parseInt(value);

            return compare(op1,op,op2);
        }
        else if(amf.type.equals("float"))
        {
            Float op1 = Float.parseFloat(attrValue);
            Float op2 = Float.parseFloat(value);
            return compare(op1,op,op2);
        }
        else
        {
            return compare(attrValue,op,value);
        }
    }

    public static boolean compare(int op1,String op,int op2)
    {
        if(op.equals("<"))
            return op1<op2;
        else if(op.equals(">"))
            return op1>op2;
        else if(op.equals("<="))
            return op1<=op2;
        else if(op.equals(">="))
            return op1>=op2;
        else if(op.equals("!="))
            return op1!=op2;
        else if(op.equals("="))
            return op1==op2;

        return false;
    }

    public static boolean compare(float op1,String op , float op2)
    {
        if(op.equals("<"))
            return op1<op2;
        else if(op.equals(">"))
            return op1>op2;
        else if(op.equals("<="))
            return op1<=op2;
        else if(op.equals(">="))
            return op1>=op2;
        else if(op.equals("!="))
            return op1!=op2;
        else if(op.equals("="))
            return op1==op2;

        return false;
    }

    public static boolean compare(String op1, String op, String op2)
    {
        int status=op1.compareToIgnoreCase(op2);

        if(op.equals(">"))
            return status>0;
        else if(op.equals("<"))
            return status<0;
        else if(op.equals(">="))
            return status>=0;
        else if(op.equals("<=") )
            return status<=0;
        else if(op.equals("!="))
            return status!=0;
        else if(op.equals("="))
            return status==0;

        return false;
    }





    //debug
    public static void print_buffer()
    {
        BufferManager.print_buffer();
    }

    public static void print_index_leaf(String indexName)
    {
        if(!CatalogManager.check_index_exist(indexName))
        {
            System.out.println("别逗了,没有此索引了");
            return ;
        }
        IndexMetaFile imf = CatalogManager.get_index_meta(indexName);
        if(imf==null)
        {
            System.out.println("读取索引meta数据失败了");
            return ;
        }
        TableMetaFile tmf = CatalogManager.get_table_meta(imf.tableName);
        if(tmf==null)
        {
            System.out.println("读取table meta file 失败鸟，尿。。。");
            return ;
        }
        AttributeMetaFile amf = new AttributeMetaFile();
        AttributeMetaFile tempAmf = new AttributeMetaFile();
        for(int i=0;i<tmf.colList.size();i++)
        {
            tempAmf = tmf.colList.get(i);
            if(tempAmf.attributeName.equals(imf.attribute))
            {
                amf = tempAmf;
                break;
            }
        }
        if(amf == null)
        {
            System.out.println("API::create_index---->can't fetch the attribute info");
            return ;
        }
        IndexContext ic = new IndexContext();
        ic.filename = imf.filename;
        ic.rootBlock = imf.rootBlock;
        ic.existBlock = new ArrayList<Integer>(imf.existBlock);
        ic.keyType = amf.type;
        ic.keyLength = amf.valueLen;

        IndexManager.set_index_context(ic);

        IndexManager.print_all_leaf();
    }

    public static void print_bplus_tree(String indexName)
    {
        if(!CatalogManager.check_index_exist(indexName))
        {
            System.out.println("别逗了,没有此索引了");
            return ;
        }
        IndexMetaFile imf = CatalogManager.get_index_meta(indexName);
        if(imf==null)
        {
            System.out.println("读取索引meta数据失败了");
            return ;
        }
        TableMetaFile tmf = CatalogManager.get_table_meta(imf.tableName);
        if(tmf==null)
        {
            System.out.println("读取table meta file 失败鸟，尿。。。");
            return ;
        }
        AttributeMetaFile amf = new AttributeMetaFile();
        AttributeMetaFile tempAmf = new AttributeMetaFile();
        for(int i=0;i<tmf.colList.size();i++)
        {
            tempAmf = tmf.colList.get(i);
            if(tempAmf.attributeName.equals(imf.attribute))
            {
                amf = tempAmf;
                break;
            }
        }
        if(amf == null)
        {
            System.out.println("API::create_index---->can't fetch the attribute info");
            return ;
        }
        IndexContext ic = new IndexContext();
        ic.filename = imf.filename;
        ic.rootBlock = imf.rootBlock;
        ic.existBlock = new ArrayList<Integer>(imf.existBlock);
        ic.keyType = amf.type;
        ic.keyLength = amf.valueLen;

        IndexManager.set_index_context(ic);

        IndexManager.print_bplus_tree();
    }

    public static void print_index_block(String indexName,int block)
    {
        if(!CatalogManager.check_index_exist(indexName))
        {
            System.out.println("别逗了,没有此索引了");
            return ;
        }
        IndexMetaFile imf = CatalogManager.get_index_meta(indexName);
        if(imf==null)
        {
            System.out.println("读取索引meta数据失败了");
            return ;
        }
        TableMetaFile tmf = CatalogManager.get_table_meta(imf.tableName);
        if(tmf==null)
        {
            System.out.println("读取table meta file 失败鸟，尿。。。");
            return ;
        }
        AttributeMetaFile amf = new AttributeMetaFile();
        AttributeMetaFile tempAmf = new AttributeMetaFile();
        for(int i=0;i<tmf.colList.size();i++)
        {
            tempAmf = tmf.colList.get(i);
            if(tempAmf.attributeName.equals(imf.attribute))
            {
                amf = tempAmf;
                break;
            }
        }
        if(amf == null)
        {
            System.out.println("API::create_index---->can't fetch the attribute info");
            return ;
        }
        IndexContext ic = new IndexContext();
        ic.filename = imf.filename;
        ic.rootBlock = imf.rootBlock;
        ic.existBlock = new ArrayList<Integer>(imf.existBlock);
        ic.keyType = amf.type;
        ic.keyLength = amf.valueLen;

        IndexManager.set_index_context(ic);

        IndexManager.print_block(block);
    }

    public static void print_meta()
    {
        CatalogManager.print_info();
    }

}
