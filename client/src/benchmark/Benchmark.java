package benchmark;

import org.voltdb.*;
import org.voltdb.client.*;

public class Benchmark {

    private Client client;

    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }
    }


    public void init() throws Exception {

        client.callProcedure(new BenchmarkCallback("HELLOWORLD.insert"),
                             "HELLOWORLD.insert",
                             0,
                             "Hello"
                             );


    }

    
    public void runBenchmark() throws Exception {

        for (int i=1; i<10000; i++) {

            client.callProcedure(new BenchmarkCallback("HELLOWORLD.insert"),
                                 "HELLOWORLD.insert",
                                 i,
                                 "Hello again"
                                 );

        }

        client.drain();

        BenchmarkCallback.printAllResults();

        client.close();
    }
    
    
    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) { serverlist = args[0]; }
        Benchmark benchmark = new Benchmark(serverlist);
        if (args.length <= 1) {
            benchmark.init();
            benchmark.runBenchmark();
        } else {
            for (int i=1; i<args.length; i++) {
                String arg = args[i];
                if (arg.equals("init")) {
                    benchmark.init();
                }
                if (arg.equals("benchmark")) {
                    benchmark.runBenchmark();
                }
            }
        }
    }
}
