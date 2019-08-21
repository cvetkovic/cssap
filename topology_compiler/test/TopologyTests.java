package compiler;

import compiler.graph.*;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
import compiler.interfaces.basic.Source;
import compiler.interfaces.lambda.Endpoint;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TopologyTests implements Serializable
{
    @Test
    public void semanticsPreservation() throws Exception
    {
        LocalCluster cluster = new LocalCluster();

        int length = 10;
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
}