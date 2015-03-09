package procedures;

import org.voltdb.*;

public class SelectHelloMsg extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "SELECT * "+
      "FROM HELLOWORLD "+
      "WHERE id = ?;"
  );

  public VoltTable[] run( int id)
      throws VoltAbortException {
          voltQueueSQL( sql, id );
          return voltExecuteSQL();
      }
}
