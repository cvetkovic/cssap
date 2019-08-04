package compiler.storm.groupings;

import compiler.interfaces.basic.Operator;
import compiler.storm.SystemMessage;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class RoundRobinSplitterGrouping implements CustomStreamGrouping
{
    private List<Integer> targetTasks;
    private String operatorName;

    public RoundRobinSplitterGrouping(String operatorName)
    {
        this.operatorName = operatorName;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        SystemMessage systemMessage = (SystemMessage) values.get(1);
        SystemMessage.MeantFor payload = (SystemMessage.MeantFor) ((SystemMessage) values.get(1)).getPayload();

        if (systemMessage.getOperatorName().equals(operatorName) && systemMessage.getOperation() == Operator.Operation.ROUND_ROBIN_SPLITTER)
        {
            if (targetTasks.contains(payload.tupleMeantFor))
                return Collections.singletonList(payload.tupleMeantFor);
            else
                return new LinkedList<>();
        }
        else
            return new LinkedList<>();
    }
}
