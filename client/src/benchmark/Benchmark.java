package benchmark;

import java.util.ArrayList;
import java.util.Random;
import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.client.VoltBulkLoader.*;
import org.voltdb.types.*;

public class Benchmark {

    private Client client;
    private ConnectionManager manager;
    private Random rand = new Random();

    public Benchmark(String servers) throws Exception {
        manager = new ConnectionManager(servers,"","");
        client = manager.getClient();
    }

    // we must implement the BulkLoaderFailureCallBack interface, there is no default implementation
    public class SessionFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            // do nothing for now
            // for a fuller example, see: https://github.com/VoltDB/voltdb/blob/master/src/frontend/org/voltdb/utils/CSVBulkDataLoader.java#L57
        }
    }


    // The failure callback provides an Object "rowHandle", which can be any object
    // The purpose is to handle the error by logging the bad row, or resubmitting the row, or whatever
    // It can either be an object that encapsulates the existing row data, or something like a POJO
    // In this example, the row data is stored as attributes, but also provided as an Object[] which simplifies the call to insertRow()
    public class Session {
        public int appid;
        public int deviceid;
        public long time;
        Object[] objectArray = {appid, deviceid, time};

        public Session(int appid, int deviceid, long time) {
            this.appid=appid;
            this.deviceid=deviceid;
            this.time=time;
        }

        public Object[] toArray() {
            return objectArray;
        }
    }


    public void init() throws Exception {
        // any initial setup (e.g. loading data) before the benchmark can go here
    }


    public void runInsertBenchmark(int testSize) throws Exception {

        // For comparison, here is typical benchmark data generation using procedure calls
        System.out.println("Benchmarking APP_SESSION.insert procedure calls...");
        long startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            long time = System.nanoTime();

            client.callProcedure(new BenchmarkCallback("APP_SESSION.insert"),
                                 "APP_SESSION.insert",
                                 appid,
                                 deviceid,
                                 time
                                 );
        }
        client.drain(); // wait for all responses to return
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");
    }

    // Here is benchmark data generation using the BulkLoader
    public void runBulkLoaderBenchmark(int testSize, int batchSize) throws Exception {

        System.out.println("Benchmarking with VoltBulkLoader...");

        // Get a BulkLoader for the table we want to load, with a given batch size and one callback handles failures for any failed batches
        VoltBulkLoader bulkLoader = client.getNewBulkLoader("app_session",batchSize, new SessionFailureCallback());

        long startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            long time = System.nanoTime();
            Session s = new Session(appid,deviceid,time);

            bulkLoader.insertRow(s, s.toArray());
        }
        bulkLoader.drain();
        client.drain();
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");

        bulkLoader.close();
    }


    // Here is benchmark data generation using a procedure that inserts multiple rows at a time
    public void runArrayProcedureBenchmark(int testSize) throws Exception {
        System.out.println("Benchmarking with BatchInsert procedure calls...");
        long startNanos = System.nanoTime();

        // accumulate a list of sessions which share the same deviceid
        ArrayList<Session> sessionList = new ArrayList<Session>();

        // reusable lists for the input parameters
        //ArrayList<Integer> appidList = new ArrayList<Integer>();
        ArrayList<Long> timeList = new ArrayList<Long>();

        int counter = 0;
        while (counter < testSize) {

            // clear
            sessionList.clear();
            timeList.clear();

            // generate some records with the same deviceid
            int deviceid = rand.nextInt(1000000);
            int batchSize = rand.nextInt(10)+1;
            for (int j=0; j<batchSize; j++) {
                int appid = rand.nextInt(50);
                long time = System.nanoTime();
                sessionList.add(new Session(appid,deviceid,time));
                counter++;
            }

            // transfer the list of sessions to lists of column values
            // integer values need to be int[] or long[] not boxed Integer[] or Long[], so must use a loop
            int[] appidArray = new int[sessionList.size()];
            for (int i=0; i< sessionList.size(); i++) { //Session s : sessionList) {
                timeList.add(sessionList.get(i).time);
                appidArray[i]=(int)sessionList.get(i).appid;
            }

            // send the batch of sessions to the BatchInsert procedure
            client.callProcedure(new BenchmarkCallback("BatchInsert"),
                                 "BatchInsert",
                                 deviceid,
                                 appidArray,
                                 timeList.toArray(new Long[0])
                                 );

        }
        client.drain();
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(counter/elapsedSeconds);
        System.out.println("Loaded "+counter+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");
    }

    // Here is benchmark data generation using a procedure that inserts multiple rows at a time
    public void runVoltTableProcedureBenchmark(int testSize) throws Exception {
        System.out.println("Benchmarking with BatchInsertVoltTable procedure calls...");
        long startNanos = System.nanoTime();

        // use a VoltTable to store multiple rows (minus the deviceid, since that is constant)
        VoltTable table = new VoltTable(new VoltTable.ColumnInfo("appid",VoltType.INTEGER),
                                        new VoltTable.ColumnInfo("time",VoltType.BIGINT)
                                        );

        int counter = 0;
        while (counter < testSize) {

            // clear
            table.clearRowData();

            // generate some records with the same deviceid
            int deviceid = rand.nextInt(1000000);
            int batchSize = rand.nextInt(10)+1;
            for (int j=0; j<batchSize; j++) {
                int appid = rand.nextInt(50);
                long time = System.nanoTime();
                table.addRow(appid,time);
                counter++;
            }

            // send the deviceid and VoltTable to the BatchInsertVoltTable procedure
            client.callProcedure(new BenchmarkCallback("BatchInsertVoltTable"),
                                 "BatchInsertVoltTable",
                                 deviceid,
                                 table
                                 );

        }
        client.drain();
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(counter/elapsedSeconds);
        System.out.println("Loaded "+counter+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");
    }

    public void runCopyToStream(int limit) throws Exception {
        System.out.println("Running CopyToStream procedure calls...");

        VoltTable partitionKeys = client.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];

        while (partitionKeys.advanceRow()) {
            long partitionId = partitionKeys.getLong("PARTITION_ID");
            long partitionKey = partitionKeys.getLong("PARTITION_KEY");
            client.callProcedure("CopyToStream",partitionKey, limit);
        }
        System.out.println("Stopped CopyToStream");
    }


    public void close() throws InterruptedException {
        //client.close();
        manager.close();
    }



    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) {
            serverlist = args[0];
        }
        Benchmark benchmark = new Benchmark(serverlist);

        int testRecordCount = 1000000;
        if (args.length > 1) {
            testRecordCount = Integer.parseInt(args[1]);
            System.out.println("testRecordCount = " + testRecordCount);
        }

        benchmark.init();
        benchmark.runInsertBenchmark(testRecordCount);
        //benchmark.runBulkLoaderBenchmark(testRecordCount,1000);
        //benchmark.runArrayProcedureBenchmark(testRecordCount);
        //benchmark.runVoltTableProcedureBenchmark(testRecordCount);
        benchmark.runCopyToStream(100000);
        benchmark.close();

    }
}
