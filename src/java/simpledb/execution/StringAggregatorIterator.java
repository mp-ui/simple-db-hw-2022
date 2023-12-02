package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.io.Serial;
import java.util.Map;

/**
 * @author prince
 * @since 2023/12/2 19:12
 */
public class StringAggregatorIterator extends AbstractAggregatorIterator {
    @Serial
    private static final long serialVersionUID = -636508715225985758L;

    private final Map<Field, Integer> countMap;

    public StringAggregatorIterator(StringAggregator aggregator, Map<Field, Integer> countMap) {
        super(aggregator, countMap.keySet());
        this.countMap = countMap;
    }

    @Override
    protected Field getAggregatorField(Aggregator.Op what, Field groupField) {
        if (what == Aggregator.Op.COUNT) {
            return new IntField(countMap.get(groupField));
        } else {
            throw new UnsupportedOperationException("unsupported operation " + what);
        }
    }
}
