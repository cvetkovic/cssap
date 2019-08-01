package compiler.storm;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.List;

public class MultichannelGrouping implements CustomStreamGrouping
{
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {

    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        return null;
    }
}