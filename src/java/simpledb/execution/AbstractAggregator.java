package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.io.Serial;
import java.util.Objects;

/**
 * @author prince
 * @since 2023/12/2 17:46
 */
public abstract class AbstractAggregator implements Aggregator {
    @Serial
    private static final long serialVersionUID = -298756475231259748L;

    private final int groupField;
    private final Type groupFieldType;
    private final int aggregateField;
    private final Op aggregateOperation;

    private TupleDesc originalTupleDesc;

    /**
     * Aggregate constructor
     *
     * @param groupField         the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param groupFieldType     the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aggregateField     the 0-based index of the aggregate field in the tuple
     * @param aggregateOperation the aggregation operator
     */
    public AbstractAggregator(int groupField, Type groupFieldType, int aggregateField, Op aggregateOperation) {
        this.groupField = groupField;
        this.groupFieldType = groupFieldType;
        this.aggregateField = aggregateField;
        this.aggregateOperation = aggregateOperation;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = NO_GROUPING_FIELD;
        if (!Objects.equals(this.groupField, NO_GROUPING) && Objects.nonNull(this.groupFieldType)) {
            groupField = tup.getField(this.groupField);
        }

        // 统计
        Field aggregateField = tup.getField(this.aggregateField);
        this.calculate(groupField, aggregateField);

        // 缓存原始的tupleDesc
        this.originalTupleDesc = tup.getTupleDesc();
    }

    /**
     * 抽象方法，用于子类实现自己的统计逻辑
     *
     * @param groupField     分组查询的字段
     * @param aggregateField 聚合查询的字段
     */
    protected abstract void calculate(Field groupField, Field aggregateField);

    /**
     * 是否是分组查询
     */
    public boolean grouping() {
        return !Objects.equals(this.groupField, NO_GROUPING) && Objects.nonNull(this.groupFieldType);
    }

    public TupleDesc getOriginalTupleDesc() {
        return originalTupleDesc;
    }

    public int getGroupField() {
        return groupField;
    }

    public Type getGroupFieldType() {
        return groupFieldType;
    }

    public int getAggregateField() {
        return aggregateField;
    }

    public Op getAggregateOperation() {
        return aggregateOperation;
    }
}
