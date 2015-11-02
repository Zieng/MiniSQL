import java.util.Scanner;

/**
 * Created by zieng on 10/4/15.
 */
public class TestRegex {
    public static void main(String [] args)
    {
        Scanner input = new Scanner(System.in);

        while(true)
        {
            String c=input.next();

            if(c.matches("^\\)[ \\t\\n\\x0B\\f\\r]*[0-9a-zA-Z]*$"))
                System.out.println("OK");
        }
    }
}
