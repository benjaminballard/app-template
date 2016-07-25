package procedures;

import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class BatchInsertVoltTable extends VoltProcedure {

    public final SQLStmt sql = new SQLStmt(
        "INSERT INTO app_session VALUES (?,?,?);"
                                           );

    public VoltTable[] run(long deviceid, VoltTable table)
        throws VoltAbortException {

        while(table.advanceRow()) {
            voltQueueSQL(sql,(int)table.getLong(0),deviceid,table.getLong(1));
        }
        return voltExecuteSQL();
    }
}
