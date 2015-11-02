/**
 * Created by zieng on 10/29/15.
 */
public class DeleteUpInfo
{
    Object deletedKey;
    Object outdatedKey;   // the key to update in parent
    Object keyForUpdate;      // the key for update in parent

    boolean deleteFisrtKey;  // if true,need delete parent's corespondent node
    boolean borrow;  // borrow some node from sibling to keep legal
    boolean merge;   // merge a sibling to keep legal

    int disappearBlock;  // after mearge the disappear block

}
