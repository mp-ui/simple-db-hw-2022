package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.io.Serial;
import java.util.Map;

/**
 * @author prince
 * @since 2023/12/2 19:05
 */
public class IntegerAggregatorIterator extends AbstractAggregatorIterator {
    @Serial
    private static final long serialVersionUID = 3392102713877177344L;

    private final Map<Field, Integer> countMap;
    private final Map<Field, Integer> minMap;
    private final Map<Field, Integer> maxMap;
    private final Map<Field, Integer> sumMap;

    public IntegerAggregatorIterator(IntegerAggregator aggregator,
                                     Map<Field, Integer> countMap,
                                     Map<Field, Integer> minMap,
                                     Map<Field, Integer> maxMap,
                                     Map<Field, Integer> sumMap) {
        super(aggregator, countMap.keySet());
        this.countMap = countMap;
        this.minMap = minMap;
        this.maxMap = maxMap;
        this.sumMap = sumMap;
    }

    @Override
    protected Field getAggregatorField(Aggregator.Op what, Field groupField) {
        if (what == Aggregator.Op.MIN) {
            return new IntField(minMap.get(groupField));
        } else if (what == Aggregator.Op.MAX) {
            return new IntField(maxMap.get(groupField));
        } else if (what == Aggregator.Op.AVG) {
            return new IntField(sumMap.get(groupField) / countMap.get(groupField));
        } else if (what == Aggregator.Op.SUM) {
            return new IntField(sumMap.get(groupField));
        } else if (what == Aggregator.Op.COUNT) {
            return new IntField(countMap.get(groupField));
        } else {
            throw new UnsupportedOperationException("unsupported operation " + what);
        }
    }
}
