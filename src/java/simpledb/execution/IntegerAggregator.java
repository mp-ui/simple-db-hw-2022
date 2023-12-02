package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AbstractAggregator {
    @Serial
    private static final long serialVersionUID = -6053561637443413959L;

    private final Map<Field, Integer> countMap;
    private final Map<Field, Integer> minMap;
    private final Map<Field, Integer> maxMap;
    private final Map<Field, Integer> sumMap;

    public IntegerAggregator(int groupField, Type groupFieldType, int aggregateField, Op aggregateOperation) {
        super(groupField, groupFieldType, aggregateField, aggregateOperation);
        this.countMap = new HashMap<>();
        this.minMap = new HashMap<>();
        this.maxMap = new HashMap<>();
        this.sumMap = new HashMap<>();
    }

    @Override
    protected void calculate(Field groupField, Field aggregateField) {
        int aggregateValue = ((IntField) aggregateField).getValue();
        // 统计个数、最小值、最大值以及总和
        countMap.put(groupField, countMap.getOrDefault(groupField, 0) + 1);
        minMap.put(groupField, Math.min(minMap.getOrDefault(groupField, Integer.MAX_VALUE), aggregateValue));
        maxMap.put(groupField, Math.max(maxMap.getOrDefault(groupField, Integer.MIN_VALUE), aggregateValue));
        sumMap.put(groupField, sumMap.getOrDefault(groupField, 0) + aggregateValue);
    }

    public OpIterator iterator() {
        return new IntegerAggregatorIterator(this, this.countMap, this.minMap, this.maxMap, this.sumMap);
    }

}
