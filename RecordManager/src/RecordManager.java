import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zieng on 10/13/15.
 */
public class RecordManager
{
    static ArrayList<LinearSearchUnit> linearSearchUnitList = null;
    static String filename;

    // this is just linear search for earlier test
    public static ArrayList<LinearSearchUnit> get_record_position_linear(String tableName,String attribute)
    {
        Gson gson = new Gson();
        filename=tableName+"."+attribute+".linear";

        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(tableName+"."+attribute+".linear"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
//                System.out.println("\t\tRecordManager::get_record_position_linear-->线性加载失败:(.....启用默认初始化设置");
                linearSearchUnitList = new ArrayList<LinearSearchUnit>();
                if(linearSearchUnitList ==null)
                {
                    System.out.println("分配了居然还是null？？");
                }
            }
            else
            {
//                System.out.println("IndexManager::get_record_position_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        // 这里使用之后不能调用close_index_manager,因为这是用来输出一系列record的函数，关掉就没法查找后续纪录的索引了。

        return linearSearchUnitList;
    }

    public static LinearSearchUnit get_record_position_linear(String tableName,String attribute, String value)
    {
        Gson gson = new Gson();
        filename=tableName+"."+attribute+".linear";

        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(tableName+"."+attribute+".linear"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
                System.out.println("IndexManager::get_record_position_linear-->线性加载失败:(.....启用默认初始化设置");
                linearSearchUnitList = new ArrayList<LinearSearchUnit>();
            }
            else
            {
//                System.out.println("IndexManager::get_record_position_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        for(int i=0;i< linearSearchUnitList.size();i++)
        {
            LinearSearchUnit iu = linearSearchUnitList.get(i);
            if(iu.value.equals(value))
            {
                return iu;
            }
        }
        close_linear_search();

        return null;
    }

    public static LinearSearchUnit insert_record_linear(String tableName,String attribute,String value,int recordLen)
    {
        Gson gson = new Gson();
//        ArrayList<LinearSearchUnit> linearSearchUnitList = new ArrayList<LinearSearchUnit>();
        filename=tableName+"."+attribute+".linear";

        linearSearchUnitList =null;
        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("IndexManager::insert_record_linear-->Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(tableName+"."+attribute+".linear"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
//                System.out.println("RecordManager::insert_record_linear-->线性索引文件加载失败:(.....启用默认初始化设置");
                linearSearchUnitList = new ArrayList<LinearSearchUnit>();
            }
            else
            {
//                System.out.println("IndexManager::insert_record_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        LinearSearchUnit x =new LinearSearchUnit();
        x.value=value;
        int maxRecordNum = 4096/recordLen;
        if(linearSearchUnitList.size()==0)
        {
//            System.out.println("RecordManager::insert_record_linear-->一条索引都没有，赶紧插入一条新的index纪录在0,0处");
            x.blockNum=x.recordNum=0;
            linearSearchUnitList.add(x);
        }
        else
        {
            for(int i=0;i< linearSearchUnitList.size();i++)
            {

            }
            int maxBlock=0;
            for(LinearSearchUnit iu: linearSearchUnitList)
            {
                if(iu.blockNum > maxBlock)
                    maxBlock=iu.blockNum;
                x.blockNum = iu.blockNum;
                x.recordNum = iu.recordNum +1;
                if(!check_record_exist(x.blockNum,x.recordNum) && iu.recordNum<maxRecordNum-1)
                {
//                    System.out.println("RecordManager::insert_record_linear-->插入了一条record在"+x.blockNum+","+x.recordNum+"处");
                    linearSearchUnitList.add(x);
                    close_linear_search();
                    return x;
                }
            }
            // all block is full, need to alloc a new block
            x.blockNum=maxBlock+1;
            x.recordNum=0;
            linearSearchUnitList.add(x);
//            System.out.println("IndexManager::insert_record_linear-->索引创建竟然失败了");
//            return null;
        }
        close_linear_search();

        return x;
    }

    public static void delete_all_linear_record(String tableName, String attribute)
    {
        Gson gson = new Gson();
//        ArrayList<LinearSearchUnit> linearSearchUnitList = new ArrayList<LinearSearchUnit>();
        filename=tableName+"."+attribute+".linear";

        linearSearchUnitList =null;
        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("IndexManager::insert_record_linear-->Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(tableName+"."+attribute+".linear"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
//                System.out.println("RecordManager::insert_record_linear-->线性索引文件加载失败:(.....启用默认初始化设置");
                linearSearchUnitList = new ArrayList<LinearSearchUnit>();
            }
            else
            {
//                System.out.println("IndexManager::insert_record_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        linearSearchUnitList.clear();   // empty the linear order record meta
        close_linear_search();
    }

    public static boolean delete_linear_record(String tableName,String attribute, int block,int record)
    {
        Gson gson = new Gson();
//        ArrayList<LinearSearchUnit> linearSearchUnitList = new ArrayList<LinearSearchUnit>();
        filename=tableName+"."+attribute+".linear";

        linearSearchUnitList =null;
        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("IndexManager::insert_record_linear-->Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(tableName+"."+attribute+".linear"));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
//                System.out.println("RecordManager::insert_record_linear-->线性索引文件加载失败:(.....启用默认初始化设置");
                linearSearchUnitList = new ArrayList<LinearSearchUnit>();
            }
            else
            {
//                System.out.println("IndexManager::insert_record_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        Iterator iter = linearSearchUnitList.iterator();
        while(iter.hasNext())
        {
            LinearSearchUnit lsu = (LinearSearchUnit )iter.next();

            if(lsu.recordNum==record && lsu.blockNum==block)
                iter.remove();
        }

        close_linear_search();

        return true;
    }
    public static boolean check_record_exist(int block_num,int record_num)
    {
        for(LinearSearchUnit iu: linearSearchUnitList)
        {
            if(iu.blockNum==block_num && iu.recordNum==record_num)
                return true;
        }
        return false;
    }

    public static void close_linear_search()
    {
        Gson gson = new Gson();
        // convert java object to JSON format,
        // and returned as JSON formatted string
        String json = gson.toJson(linearSearchUnitList);
        try
        {
//            System.out.println("\t\tRecordManager::close_linear_search-->write linear meta data to file......");
            FileWriter writer = new FileWriter(filename);
            writer.write(json);
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        linearSearchUnitList =null;
    }

    public static void print_info_linear(String index_fine_name)
    {
        filename = index_fine_name;
        Gson gson = new Gson();
        try
        {
            ArrayList<LinearSearchUnit> temp;
//            System.out.println("IndexManager::insert_record_linear-->Loading data.....");
            BufferedReader br = new BufferedReader(
                    new FileReader(filename));
//            temp = gson.fromJson(br, ArrayList.class);
            temp = gson.fromJson(br,new TypeToken<List<LinearSearchUnit>>(){}.getType());
            if(temp==null)
            {
                System.out.println("IndexManager::insert_record_linear-->线性索引文件加载失败");
            }
            else
            {
//                System.out.println("IndexManager::insert_record_linear-->初始化成功");
                linearSearchUnitList =temp;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println("------------"+filename+"------------");
        for(LinearSearchUnit iu: linearSearchUnitList)
        {
            System.out.println("\tvalue="+iu.value+",block_num="+iu.blockNum+",record_num"+iu.recordNum);
        }
        close_linear_search();
    }




    //
    public static boolean insert_record_into_block(String filename, int block_num, int record_num, int recordSize, byte[] recordData)
    {
        byte [] tempBlock = BufferManager.get_block_readwrite(filename,block_num);
        byte [] tempRecord = new byte[recordSize];

        if(tempBlock==null)
        {
            System.out.println("get_block return null");
            return false;
        }
        if(recordData.length!=recordSize)
        {
            System.out.println("In RecordManager::insert_record_into_block-->数据的纪录长度不符要求");
        }
        if(tempBlock.length!=4096)
        {
            System.out.println("In RecordManager::insert_record_into_block-->取得的块长度不符要求");
        }
//        System.out.println("\tTest: the length of tempBlock="+tempBlock.length);
//        System.out.println("\t the record num="+record_num+",the record size="+recordSize);
//        System.out.println("\tTest: the length of recordData="+recordData.length);
        System.arraycopy(recordData, 0, tempBlock, record_num * recordSize, recordSize);
        System.arraycopy(tempBlock,record_num*recordSize,tempRecord,0,recordSize);
        if( !Arrays.equals(tempRecord, recordData) )
            return false;

        BufferManager.return_block(filename,block_num,true);
        return true;
    }

    public static boolean select_record_from_blcok(String filename, int block_num, int record_num, int recordSize, byte[] recordData)
    {
        byte [] tempBlock = BufferManager.get_block_readonly(filename, block_num);

        if(tempBlock==null)
        {
            System.out.println("get_block return null");
            return false;
        }
        System.arraycopy(tempBlock,record_num*recordSize, recordData,0,recordSize);

        BufferManager.return_block(filename,block_num,false);
        return true;
    }

    public static boolean insert_record(String tableName,ArrayList<AttributeMetaFile> attrList,ArrayList<String> valueList,int block_num,int record_num,int recordSize)
    {
        ByteArrayOutputStream byteOutPutStream = new ByteArrayOutputStream();

        for(int i=0;i<attrList.size();i++)
        {
            AttributeMetaFile amf = attrList.get(i);
//            System.out.println(amf.type);
            byte [] temp;

            if(amf.type.equals("int"))
            {
                Integer intValue = Integer.parseInt(valueList.get(i));
                temp = ByteBuffer.allocate(4).putInt(intValue).array();
//                System.out.println("add 4 bytes");
            }
            else if(amf.type.equals("float"))
            {
                Float floatValue = Float.parseFloat(valueList.get(i));
                temp = ByteBuffer.allocate(4).putFloat(floatValue).array();
//                System.out.println("add 4 bytes");
            }
            else
            {
                String s=valueList.get(i);
                while(s.length()<amf.valueLen)
                    s+='\0';

                temp = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
//                System.out.println("add "+temp.length+" bytes");
                if(temp.length!=amf.valueLen)
                    System.out.println("RecordManager:insert_record!!!!!!!!!!!!!!!!!!");
            }

            try
            {
                byteOutPutStream.write(temp);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        byte [] data = byteOutPutStream.toByteArray();

//        System.out.println("the data has the length:"+data.length);

        return insert_record_into_block(tableName+".dbfile",block_num,record_num,recordSize,data);
    }

    public static boolean select_record(String tableName,ArrayList<AttributeMetaFile> attrList,int block_num,int record_num,int recordSize)
    {
        byte [] data = new byte[recordSize];
        String result="";

        if(!select_record_from_blcok(tableName+".dbfile",block_num,record_num,recordSize,data))
        {
            System.out.println("RecordManager 在从块中读取数据时崩了");
            return false;
        }
        if(data==null || data.length!=recordSize)
        {
            System.out.println("RecordManager 在从块中读取数据时读傻了");
            return false;
        }

        for (int i=0;i<attrList.size();i++)
        {
            AttributeMetaFile amf = attrList.get(i);
            result+="|\t";
            byte [] temp;

            if(amf.type.equals("int"))
            {
                temp = Arrays.copyOfRange(data,0,4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Integer intValue = wrapped.getInt();
                result+=intValue.toString();
                data=Arrays.copyOfRange(data,4,data.length);
            }
            else if(amf.type.equals("float"))
            {
                temp = Arrays.copyOfRange(data,0,4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Float floatValue = wrapped.getFloat();
                result+=floatValue.toString();
                data=Arrays.copyOfRange(data, 4, data.length);
            }
            else
            {
                temp = Arrays.copyOfRange(data,0,amf.valueLen);
                String strValue = new String(temp,java.nio.charset.StandardCharsets.UTF_8);
//                ByteBuffer wrapped = ByteBuffer.wrap(temp);
//                String strValue = wrapped.toString();
                result+=strValue.toString().trim();
                data=Arrays.copyOfRange(data, amf.valueLen, data.length);
            }
            result+="\t";
        }
        System.out.println(result+"|");

        return true;
    }

    public static boolean select_record_condition(String tableName,ArrayList<AttributeMetaFile> attrList, String condition,int block_num,int record_num,int recordSize)
    {
        ArrayList<String> valueList = new ArrayList<String>(attrList.size());
        ArrayList<String> conditionList = new ArrayList<String>(Arrays.asList(condition.split("and")));


        byte[] data = new byte[recordSize];

        //first fetch the record from the block
        if(!select_record_from_blcok(tableName+".dbfile",block_num,record_num,recordSize,data))
        {
            System.out.println("RecordManager 在从块中读取数据时崩了");
            return false;
        }
        if(data==null || data.length!=recordSize)
        {
            System.out.println("RecordManager 在从块中读取数据时读傻了");
            return false;
        }

        for (int i=0;i<attrList.size();i++)         // fetch record from byte stream
        {
            AttributeMetaFile amf = attrList.get(i);
            String result;
            byte[] temp;

            if (amf.type.equals("int")) {
                temp = Arrays.copyOfRange(data, 0, 4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Integer intValue = wrapped.getInt();
                result = intValue.toString();
                data = Arrays.copyOfRange(data, 4, data.length);
            } else if (amf.type.equals("float")) {
                temp = Arrays.copyOfRange(data, 0, 4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Float floatValue = wrapped.getFloat();
                result = floatValue.toString();
                data = Arrays.copyOfRange(data, 4, data.length);
            } else {
                temp = Arrays.copyOfRange(data, 0, amf.valueLen);
                String strValue = new String(temp, java.nio.charset.StandardCharsets.UTF_8);
//                ByteBuffer wrapped = ByteBuffer.wrap(temp);
//                String strValue = wrapped.toString();
                result = strValue.toString().trim();
                data = Arrays.copyOfRange(data, amf.valueLen, data.length);
            }
            valueList.add(result);
        }

        // pipeline to parse the result to get the required record
        boolean pass = true;
        ArrayList<String> attrNameList = new ArrayList<String>();
        for(int i=0;i<attrList.size();i++)
        {
            attrNameList.add(attrList.get(i).attributeName);
        }

        for(String cond:conditionList)            // parse the condition operator
        {
            cond=cond.trim();
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
                System.out.println("RecordManager::select_record_condition-->哎呀，你的where条件语句没写对");
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
            if(index <0 || index>=attrList.size())
            {
                System.out.println(Attr+"对应的index＝"+index);
                System.out.println("\t\tRecordManager::select_record_condition-->表"+tableName+"里貌似没有"+Attr+"这个属性吧？");
                return false;
            }

            if(!check_condition_match(attrList.get(index),valueList.get(index),op,Value))
            {
                pass=false;
                break;
            }
        }
        if(pass)
        {
            String result="";
            for(int i=0;i<valueList.size();i++)
            {
                result+="\t"+valueList.get(i)+"\t|";
            }
            System.out.println(result);
        }

        return true;
    }

    public static ArrayList<String> delete_record_condition(String tableName,String pk,ArrayList<AttributeMetaFile> attrList, String condition,int block_num,int record_num,int recordSize)
    {
        ArrayList<String> valueList = new ArrayList<String>(attrList.size());
        ArrayList<String> conditionList = new ArrayList<String>(Arrays.asList(condition.split("and")));


        byte[] data = new byte[recordSize];

        //first fetch the record from the block
        if(!select_record_from_blcok(tableName+".dbfile",block_num,record_num,recordSize,data))
        {
            System.out.println("RecordManager 在从块中读取数据时崩了");
            return null;
        }
        if(data==null || data.length!=recordSize)
        {
            System.out.println("RecordManager 在从块中读取数据时读傻了");
            return null;
        }

//        System.out.println("\t\tRecordManager::delete_record_condition--> fetch record from byte stream");
        for (int i=0;i<attrList.size();i++)         // fetch record from byte stream
        {
            AttributeMetaFile amf = attrList.get(i);
            String result;
            byte[] temp;

            if (amf.type.equals("int")) {
                temp = Arrays.copyOfRange(data, 0, 4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Integer intValue = wrapped.getInt();
                result = intValue.toString();
                data = Arrays.copyOfRange(data, 4, data.length);
            } else if (amf.type.equals("float")) {
                temp = Arrays.copyOfRange(data, 0, 4);
                ByteBuffer wrapped = ByteBuffer.wrap(temp);
                Float floatValue = wrapped.getFloat();
                result = floatValue.toString();
                data = Arrays.copyOfRange(data, 4, data.length);
            } else {
                temp = Arrays.copyOfRange(data, 0, amf.valueLen);
                String strValue = new String(temp, java.nio.charset.StandardCharsets.UTF_8);
//                ByteBuffer wrapped = ByteBuffer.wrap(temp);
//                String strValue = wrapped.toString();
                result = strValue.toString().trim();
                data = Arrays.copyOfRange(data, amf.valueLen, data.length);
            }
            valueList.add(result);
        }

        // pipeline to parse the result to get the required record
        boolean pass = true;
        ArrayList<String> attrNameList = new ArrayList<String>();
        for(int i=0;i<attrList.size();i++)
        {
            attrNameList.add(attrList.get(i).attributeName);
        }

        for(String cond:conditionList)            // parse the condition operator
        {
            cond=cond.trim();
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
                System.out.println("\t\tRecordManager::delete_record_condition-->哎呀，你的where条件语句没写对");
                return null;
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
            if(index <0 || index>=attrList.size())
            {
                System.out.println("\t\t"+Attr+"对应的index＝"+index);
                System.out.println("\t\tRecordManager::delete_record_condition-->表"+tableName+"里貌似没有"+Attr+"这个属性吧？");
                return null;
            }

            if(!check_condition_match(attrList.get(index),valueList.get(index),op,Value))
            {
//                System.out.println("\t\tRecord "+valueList.toString()+" didn't match "+op+" "+Value);
                pass=false;
                break;
            }
        }
        if(pass)   // if record match all condition
        {
            String result="Going to delete:";
            for(int i=0;i<valueList.size();i++)
            {
                result+="\t"+valueList.get(i)+"\t|";
            }
            System.out.println(result);
            delete_linear_record(tableName,pk,block_num,record_num);

            return valueList;
        }

        return null;
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
}
