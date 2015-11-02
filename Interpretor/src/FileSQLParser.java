import java.io.*;

/**
 * Created by zieng on 9/28/15.
 */
public class FileSQLParser
{
    static String currentFile;

    public static boolean get_filename(String filename)
    {
        currentFile = filename;

        System.out.println();
        System.out.println("--------------------------------不要慌，正在打开" + filename + ".....---------------------------");
        System.out.println();
        File file = new File(filename);
        if(!file.exists())
        {
            File directory = new File("");
            System.out.println(directory.getAbsolutePath());
            System.out.println("No such files!"+file.getAbsolutePath());

            return false;
        }
        InputStream in;
        try
        {
            int sqlCount=1;
            // 一次读一个字节
            in = new FileInputStream(file);
            int tempbyte;
            String sql="";
            while ((tempbyte = in.read()) != -1)
            {
                if(tempbyte!=';')
                {
                    sql+=(char)tempbyte;
                }
                else
                {
                    sql = sql.trim();
                    System.out.println("正在执行第"+sqlCount+"条SQL命令："+sql);
                    System.out.println();

                    if(sql.contains("execfile "+currentFile) )
                    {
                        System.out.println("错误！脚本文件"+currentFile+"试图调用执行:execfile "+currentFile);
                        return false;
                    }

                    InterpretorManager.command_handler(sql);
                    if(sql.equals("quit"))
                        break;

                    sql="";
                    sqlCount++;
                }
            }
            in.close();

            return true;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }
}
