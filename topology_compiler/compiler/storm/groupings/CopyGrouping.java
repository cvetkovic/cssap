package compiler.storm.groupings;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.LinkedList;
import java.util.List;

public class CopyGrouping implements CustomStreamGrouping
{
    private List<Integer> targetTasks = new LinkedList<>();

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {
        this.targetTasks.addAll(targetTasks);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        // sends to everybody subscribed
        return targetTasks;
    }
}