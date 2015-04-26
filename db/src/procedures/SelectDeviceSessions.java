package procedures;

import org.voltdb.*;

public class SelectDeviceSessions extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "SELECT * "+
      "FROM app_session "+
      "WHERE deviceid = ? "+
      "ORDER BY ts, appid, deviceid;"
  );

  public VoltTable[] run(long deviceid)
      throws VoltAbortException {
          voltQueueSQL( sql, deviceid );
          return voltExecuteSQL();
      }
}
