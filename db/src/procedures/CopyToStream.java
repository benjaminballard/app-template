package procedures;

import java.util.Date;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

/*
   Goals:
    1. Copy all records within this partition from the table to the stream
    2. Do it again seamlessly (with no gaps or overlap)
    3. Limit the number of rows to copy each time.

 */



public class CopyToStream extends VoltProcedure {

    // tracking table
    public final SQLStmt getLastTime = new SQLStmt("SELECT last_time_nanos FROM app_session_tracking;");
    public final SQLStmt updateLastTime = new SQLStmt("UPDATE app_session_tracking SET last_time_nanos = ?");
    public final SQLStmt insertLastTime = new SQLStmt("INSERT INTO app_session_tracking VALUES (?,?);");


    // examine app_sessions table
    public final SQLStmt countAvailable = new SQLStmt("SELECT COUNT(*) FROM app_session WHERE time_nanos > ? AND time_nanos <= ?;");
    public final SQLStmt getOffsetTime = new SQLStmt("SELECT time_nanos FROM app_session WHERE time_nanos > ? OFFSET ? LIMIT 1;");

    // copy data
    public final SQLStmt copyToStream = new SQLStmt("INSERT INTO app_session_stream SELECT * FROM app_session WHERE time_nanos > ? AND time_nanos <= ?;");



    public VoltTable[] run(long dummyDeviceId, int limit)
        throws VoltAbortException {

        // get last time
        voltQueueSQL(getLastTime);
        VoltTable timeTable = voltExecuteSQL()[0];
        long lastTime = 0l;
        if (timeTable.advanceRow()) {
            // table was not empty, get the value
            lastTime = timeTable.getLong(0);
        } else {
            // table was empty, populate it set lastTime=0
            voltQueueSQL(insertLastTime,dummyDeviceId,lastTime);
            voltExecuteSQL();
        }

        // calculate end time --- this needs to be a few milliseconds ago to ensure records still coming in won't be skipped
        // use getTransactionTime() because it is a deterministic time value
        Date now = getTransactionTime();
        long nowMillis = now.getTime();
        long endTimeMillis = nowMillis - 500; // 500 milliseconds ago
        long endTimeNanos = endTimeMillis * 1000 * 1000;


        // check how many rows are available since lastTime
        voltQueueSQL(countAvailable, lastTime, endTimeNanos);
        VoltTable countTable = voltExecuteSQL()[0];
        countTable.advanceRow();
        long count = countTable.getLong(0);

        // if there are more than the limit, get a new end time
        if (count > limit) {
            voltQueueSQL(getOffsetTime,lastTime,limit);
            VoltTable offsetTable = voltExecuteSQL()[0];
            offsetTable.advanceRow();
            endTimeNanos = offsetTable.getLong(0);
        }

        // copy from the table to the stream and update the lastTime in the tracking table
        voltQueueSQL(copyToStream,lastTime, endTimeNanos);
        voltQueueSQL(updateLastTime,endTimeNanos);
        return voltExecuteSQL(true);
    }
}
