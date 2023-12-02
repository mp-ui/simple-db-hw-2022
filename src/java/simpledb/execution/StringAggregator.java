package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator {
    @Serial
    private static final long serialVersionUID = -7028510852937184307L;

    private final Map<Field, Integer> countMap;

    public StringAggregator(int groupField, Type groupFieldType, int aggregateField, Op aggregateOperation) {
        super(groupField, groupFieldType, aggregateField, aggregateOperation);
        this.countMap = new HashMap<>();
    }

    @Override
    protected void calculate(Field groupField, Field aggregateField) {
        // 统计个数
        countMap.put(groupField, countMap.getOrDefault(groupField, 0) + 1);
    }

    public OpIterator iterator() {
        return new StringAggregatorIterator(this, this.countMap);
    }

}
