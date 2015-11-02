/**
 * Created by zieng on 10/13/15.
 */
public class TestRecordManager
{
    public static void main(String [] args)
    {
        BufferManager BF = new BufferManager(2);
        RecordManager RM = new RecordManager();
        byte [] r = new byte[1];

        r[0]=0xf;
        RM.insert_record_into_block("recordfile_01.dbfile", 0, 0, 1, r);
        r[0]=0x2;
        RM.select_record_from_blcok("recordfile_01.dbfile", 0, 0, 1, r);
        System.out.println(r[0]);
        BF.close_buffer();
    }



}
