package compiler;

import compiler.graph.*;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
import compiler.interfaces.basic.Source;
import compiler.interfaces.lambda.Endpoint;
import compiler.interfaces.lambda.Function0;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class TopologyTests implements Serializable
{
    // sequence numbering with no dropping of expression and check that every two items are identical
    @Test
    public void semanticsPreservationTest1() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10000;
        List<String> results = new ArrayList<>();
        Random r = new Random();

        Source source = new FiniteSource(() -> r.nextDouble(), length);
        Operator copy = NodesFactory.copy("copy", 2);
        Operator map1 = NodesFactory.map("map1", (Double item) -> item);
        Operator map2 = NodesFactory.map("map2", (Double item) -> item);
        Operator merge = NodesFactory.merge("merger", 2);
        Sink printer = NodesFactory.sink("printer", new Endpoint<Object>()
        {
            private Socket socket;
            private OutputStream os;
            private DataOutputStream dos;

            @Override
            public void call(Object item)
            {
                try
                {
                    if (socket == null)
                    {
                        socket = new Socket("localhost", 5000);
                        os = socket.getOutputStream();
                        dos = new DataOutputStream(os);
                    }

                    dos.writeUTF(item.toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        ParallelGraph parallelGraph = new ParallelGraph(new AtomicGraph(map1),
                new AtomicGraph(map2));

        SerialGraph serialGraph = new SerialGraph(new AtomicGraph(copy), parallelGraph, new AtomicGraph(merge));
        Operator op = serialGraph.getOperator();
        op.subscribe(printer);

        cluster.submitTopology("semanticsPreservation", new Config(), serialGraph.getStormTopology(source));

        ServerSocket serverSocket = new ServerSocket(5000);
        Socket s = serverSocket.accept();
        DataInputStream dis = new DataInputStream(s.getInputStream());

        while (true)
        {
            String readUTF = dis.readUTF();
            if (readUTF.equals("END_OF_STREAM"))
                break;
            else
                results.add(readUTF);
        }

        for (int i = 0; i < length; i++)
        {
            String first = results.get(2 * i);
            String second = results.get(2 * i + 1);

            if (!first.equals(second))
                Assert.assertTrue(false);
        }

        cluster.killTopology("semanticsPreservation");
        Assert.assertTrue(true);
    }

    // tuple selectivity zero test
    @Test
    public void semanticsPreservationTest2() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10000;
        Random r = new Random();

        Source source = new FiniteSource(() -> r.nextDouble(), length);
        Operator multiplier = NodesFactory.map("multiplier", (Double item) -> item * 3);
        Operator copy = NodesFactory.copy("copy", 3);
        Operator filter1 = NodesFactory.filter("filter1", (Double item) -> item < 1);
        Operator filter2 = NodesFactory.filter("filter2", (Double item) -> item >= 1 && item < 2);
        Operator filter3 = NodesFactory.filter("filter3", (Double item) -> item >= 2 && item < 3);
        Operator merge = NodesFactory.merge("merger", 3);
        Sink printer = NodesFactory.sink("printer", new Endpoint<Object>()
        {
            private Socket socket;
            private OutputStream os;
            private DataOutputStream dos;

            @Override
            public void call(Object item)
            {
                try
                {
                    if (socket == null)
                    {
                        socket = new Socket("localhost", 5000);
                        os = socket.getOutputStream();
                        dos = new DataOutputStream(os);
                    }

                    dos.writeUTF(item.toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        ParallelGraph parallelGraph = new ParallelGraph(new AtomicGraph(filter1),
                new AtomicGraph(filter2),
                new AtomicGraph(filter3));

        SerialGraph serialGraph = new SerialGraph(new AtomicGraph(multiplier), new AtomicGraph(copy), parallelGraph, new AtomicGraph(merge));
        Operator op = serialGraph.getOperator();
        op.subscribe(printer);

        cluster.submitTopology("semanticsPreservation", new Config(), serialGraph.getStormTopology(source));

        ServerSocket serverSocket = new ServerSocket(5000);
        Socket s = serverSocket.accept();
        DataInputStream dis = new DataInputStream(s.getInputStream());

        int received = 0;

        while (true)
        {
            String readUTF = dis.readUTF();
            if (readUTF.equals("END_OF_STREAM"))
                break;
            else
                received++;
        }

        if (received != length)
            Assert.assertTrue(false);

        cluster.killTopology("semanticsPreservation");
        Assert.assertTrue(true);
    }

    // order preservation test
    @Test
    public void semanticsPreservationTest3() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10000;

        Source source = new FiniteSource(new Function0()
        {
            private int k = 0;

            @Override
            public synchronized Object call()
            {
                return k++;
            }
        }, length);
        Operator splitter = NodesFactory.robinRoundSplitter("splitter", 2);
        Operator filter1 = NodesFactory.map("map1", (Integer item) -> item);
        Operator filter2 = NodesFactory.map("map2", (Integer item) -> item);
        Operator merge = NodesFactory.merge("merger", 2);
        Sink printer = NodesFactory.sink("printer", new Endpoint<Object>()
        {
            private Socket socket;
            private OutputStream os;
            private DataOutputStream dos;

            @Override
            public void call(Object item)
            {
                try
                {
                    if (socket == null)
                    {
                        socket = new Socket("localhost", 5000);
                        os = socket.getOutputStream();
                        dos = new DataOutputStream(os);
                    }

                    dos.writeUTF(item.toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        ParallelGraph parallelGraph = new ParallelGraph(new AtomicGraph(filter1), new AtomicGraph(filter2));

        SerialGraph serialGraph = new SerialGraph(new AtomicGraph(splitter), parallelGraph, new AtomicGraph(merge));
        Operator op = serialGraph.getOperator();
        op.subscribe(printer);

        cluster.submitTopology("semanticsPreservation", new Config(), serialGraph.getStormTopology(source));

        ServerSocket serverSocket = new ServerSocket(5000);
        Socket s = serverSocket.accept();
        DataInputStream dis = new DataInputStream(s.getInputStream());

        int received = 0;
        List<Integer> results = new LinkedList<>();

        while (true)
        {
            String readUTF = dis.readUTF();
            if (readUTF.equals("END_OF_STREAM"))
                break;
            else
                results.add(Integer.parseInt(readUTF));
        }

        if (results.size() != length)
            Assert.assertTrue(false);

        for (int i = 0; i < length; i++)
        {
            if (results.get(i) != i)
                Assert.assertTrue(false);
        }

        cluster.killTopology("semanticsPreservation");
        Assert.assertTrue(true);
    }

    // arbitrary topology semantics preservation test
    @Test
    public void semanticsPreservationTest4() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10000;

        Source source = new FiniteSource(new Function0()
        {
            private int k = 0;

            @Override
            public synchronized Object call()
            {
                return k++;
            }
        }, length);
        Operator copy1 = NodesFactory.copy("copy1", 2);
        Operator copy2 = NodesFactory.copy("copy2", 2);
        Operator map1 = NodesFactory.map("map1", (Integer item) -> item);
        Operator map2 = NodesFactory.map("map2", (Integer item) -> item);
        Operator map3 = NodesFactory.map("map3", (Integer item) -> item);
        Operator merger1 = NodesFactory.merge("merger1", 2);
        Operator merger2 = NodesFactory.merge("merger2", 2);
        Sink printer = NodesFactory.sink("printer", new Endpoint<Object>()
        {
            private Socket socket;
            private OutputStream os;
            private DataOutputStream dos;

            @Override
            public void call(Object item)
            {
                try
                {
                    if (socket == null)
                    {
                        socket = new Socket("localhost", 5000);
                        os = socket.getOutputStream();
                        dos = new DataOutputStream(os);
                    }

                    dos.writeUTF(item.toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        ParallelGraph pg1 = new ParallelGraph(new AtomicGraph(map1), new AtomicGraph(map2));
        SerialGraph upper = new SerialGraph(new AtomicGraph(copy2), pg1, new AtomicGraph(merger2));

        ParallelGraph parallelGraph = new ParallelGraph(upper, new AtomicGraph(map3));

        SerialGraph serialGraph = new SerialGraph(new AtomicGraph(copy1), parallelGraph, new AtomicGraph(merger1));
        Operator op = serialGraph.getOperator();
        op.subscribe(printer);

        cluster.submitTopology("semanticsPreservation", new Config(), serialGraph.getStormTopology(source));

        ServerSocket serverSocket = new ServerSocket(5000);
        Socket s = serverSocket.accept();
        DataInputStream dis = new DataInputStream(s.getInputStream());

        List<Integer> results = new LinkedList<>();

        while (true)
        {
            String readUTF = dis.readUTF();
            if (readUTF.equals("END_OF_STREAM"))
                break;
            else
                results.add(Integer.parseInt(readUTF));
        }

        for (int i = 0; i < length; i++)
        {
            int first = results.get(3 * i);
            int second = results.get(3 * i + 1);
            int third = results.get(3 * i + 2);

            if (first == second && first == third)
                continue;
            else
                Assert.assertTrue(false);
        }

        cluster.killTopology("semanticsPreservation");
        Assert.assertTrue(true);
    }

    // arbitrary topology semantics preservation test
    @Test
    public void semanticsPreservationTest5() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10000;

        Source source = new FiniteSource(new Function0()
        {
            private int k = 0;

            @Override
            public synchronized Object call()
            {
                return k++;
            }
        }, length);
        Operator copy1 = NodesFactory.copy("copy", 2);
        Operator map1 = NodesFactory.map("map", (Integer item) -> item * 10);
        Operator merger1 = NodesFactory.merge("merger", 2);
        Sink printer = NodesFactory.sink("printer", new Endpoint<Object>()
        {
            private Socket socket;
            private OutputStream os;
            private DataOutputStream dos;

            @Override
            public void call(Object item)
            {
                try
                {
                    if (socket == null)
                    {
                        socket = new Socket("localhost", 5000);
                        os = socket.getOutputStream();
                        dos = new DataOutputStream(os);
                    }

                    dos.writeUTF(item.toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        copy1.subscribe(map1, merger1);
        map1.subscribe(merger1);
        merger1.subscribe(printer);

        cluster.submitTopology("semanticsPreservation", new Config(), new AtomicGraph(copy1).getStormTopology(source));

        ServerSocket serverSocket = new ServerSocket(5000);
        Socket s = serverSocket.accept();
        DataInputStream dis = new DataInputStream(s.getInputStream());

        List<Integer> results = new LinkedList<>();

        while (true)
        {
            String readUTF = dis.readUTF();
            if (readUTF.equals("END_OF_STREAM"))
                break;
            else
                results.add(Integer.parseInt(readUTF));
        }

        for (int i = 0; i < length; i++)
        {
            int first = results.get(2 * i);
            int second = results.get(2 * i + 1);

            if (first == 10 * second)
                continue;
            else
                Assert.assertTrue(false);
        }

        cluster.killTopology("semanticsPreservation");
        Assert.assertTrue(true);
    }
}