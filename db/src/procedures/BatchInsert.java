package procedures;

import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class BatchInsert extends VoltProcedure {

    public final SQLStmt sql = new SQLStmt(
        "INSERT INTO app_session VALUES (?,?,?);"
                                           );

    public VoltTable[] run(long deviceid, int[] appid, TimestampType[] ts)
        throws VoltAbortException {

        for (int i=0; i<appid.length; i++) {
            voltQueueSQL( sql, appid[i], deviceid, ts[i] );
        }
        return voltExecuteSQL();
    }
}
