import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Created by zieng on 9/29/15.
 */
public class InterpretorManager
{
    static boolean quit =false;


    public  static void main(String[] args)
    {
        API_Manager api = new API_Manager();
        Scanner input = new Scanner(System.in);
//        String lastCommand="none";
        String command="help";


        if(true)    // print the logo
        {
            System.out.println("                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "    ;G                                                                                                         .;rssi.       \n" +
                    "    X@S             S3                  ,                                    ,:;;.          ,r1h55S335i        ,;&@A.        \n" +
                    "   i@@@;           G@8       .          HX:          :i9Gs,                585hhSX8       iG8hr:,.,;s9BM9:       s@A         \n" +
                    "   AX1@A         :AB@G      .rA&5:     ,#@@S.          A@.     ,;;i;      AM,    ,5      9#1          .3@@S      ,@@:        \n" +
                    "  s@i &@3       sM1s@G       .@@i      ;#sG@M1         &M      .S@B:     ,@B,           G@s             1@@5      A@1        \n" +
                    "  BA  :@@i     3Bi 5@G       :@#,      r@, rH@Ai       8H       s@A       5@@9;        r@M               &@M.     3@8        \n" +
                    " r@r   5@B.  .&&.  S@G       s@B       h@:   h#@G:     9H       1@A        iX@@H5,     S@M.              h@#,     s@B        \n" +
                    ".HH     X@8 iB3    S@X       5@X       S@,    .3@@9,   8A       s@A          ,5B@@S    r@@1              5@X      ,#@;       \n" +
                    "A@s     ,M@XHs     5@X       9@3       8@.      :8@@S  8&       r@H             rH@8    8@#;            .#H,       A@9     .1\n" +
                    "88s:     i@A,      3@A       A@1       XM         ;X@M18X       r@M      ,       ,@B     3@@3,         r&3        ,A@#9hh553G\n" +
                    "          :        SHHh:.  .r@@S.     ,@@,          ;&@@G       1@#.     59.    ,3#s      :3BBGSsrirs59BM831;.    ,;:,:,..   \n" +
                    "                      ,,.  .;;ii;.   ;s391;.          iX3      ,3HAs,    iAX35h53S,          ,r1h1ssi:. ;h9&HHAG35s;:::::;irr\n" +
                    "                                                               .   .       .,,,.                             :rh38XXXG8351r;.\n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n" +
                    "                                                                                                                             \n");
        }

        System.out.println("---------------Welcome to use the MiniSQL------------");
        System.out.println("*******type help to get help******** ");

        while(true)
        {
            StringBuilder sb = new StringBuilder();

            System.out.print("MiniSQL->");

            while (sb.indexOf(";") == -1 && input.hasNextLine())
            {
                command = input.nextLine();
//                command = input.next();
                command = command.trim();
//                char [] c= command.toCharArray();
//                if(c[0]==38)   // up arrow
//                {
//                    command = lastCommand;
//                    break;
//                }
                if (command.equals("help") || command.equals("quit") || command.contains("execfile") || command.contains("debug") )
                {
                    command_handler(command);
                    System.out.println();
                    break;
                }
                else if( command.contains(";") )
                {
                    sb.append(" "+command);
                    command_handler(sb.toString());
                    System.out.println();
                    break;
                }

                sb.append(" "+command);

                System.out.print("\t->");
            }

            if(quit)
                break;
        }

        API_Manager.close_api();
        System.out.println("Bye~");
    }

    public static void command_handler(String command)
    {
        if(command.equals("quit"))
            quit = true;
        else if(command.equals("help"))
            print_help();
        else if (command.contains("execfile"))
        {
            // do execfile
            List<String> commandList = Arrays.asList(command.split("\\s{1,}"));
//                System.out.println(commandList.get(0));
//                System.out.println(commandList.get(1));
            if(commandList.size()==2)
            {
                FileSQLParser.get_filename(commandList.get(1));
            }
            else
            {
                System.out.println("Invalid command! Usage: execfile filename");
            }
        }
        else if( command.contains("debug"))
        {
            List<String> commandList = Arrays.asList(command.split("\\s{1,}"));
//                System.out.println(commandList.get(0));
//                System.out.println(commandList.get(1));
            if(commandList.size()==2)
                debug_function(commandList.get(1));
            else if(commandList.size()==3)
                debug_function(commandList.get(1),commandList.get(2));
            else if(commandList.size()==4)
                debug_function(commandList.get(1),commandList.get(2),commandList.get(3));
            else
                System.out.println("Invalid command! Usage: debug command");
        }
        else      // the sql command
        {
//            System.out.println("???"+command);
            SingleSQLParser.get_sql_string(command);
        }
    }

    public static void print_help(){
        System.out.println("I won't help you!");
    }

    public static void debug_function(String command)
    {
        if(command.equals("show_buffer"))
        {
            System.out.println("国家机密，不给你看");
            API_Manager.print_buffer();
        }
        else if(command.equals("show_meta"))
        {
            System.out.println("天机不可泄露");
            API_Manager.print_meta();
        }
    }

    public static void debug_function(String command,String index)
    {
        if(command.equals("show_index_leaf"))
            API_Manager.print_index_leaf(index);
        else if(command.equals("show_bplus_tree"))
            API_Manager.print_bplus_tree(index);
    }

    public static void debug_function(String command,String index,String block)
    {
        Integer blockNum = Integer.parseInt(block);

        if(command.equals("show_index_block"))
            API_Manager.print_index_block(index,blockNum);
    }
}
