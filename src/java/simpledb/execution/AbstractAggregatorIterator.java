package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author prince
 * @since 2023/12/2 18:00
 */
public abstract class AbstractAggregatorIterator extends Operator {
    @Serial
    private static final long serialVersionUID = -3485938892417253413L;

    private final AbstractAggregator aggregator;
    private final Collection<Field> groupingFields;
    private final TupleDesc tupleDesc;

    private Iterator<Field> groupingIterator;

    public AbstractAggregatorIterator(AbstractAggregator aggregator, Collection<Field> groupingFields) {
        this.aggregator = aggregator;
        this.groupingFields = groupingFields;

        // 初始化TupleDesc
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        // 分组查询
        if (aggregator.grouping()) {
            types.add(aggregator.getOriginalTupleDesc().getFieldType(aggregator.getGroupField()));
            names.add(aggregator.getOriginalTupleDesc().getFieldName(aggregator.getGroupField()));
        }

        // 聚合查询的字段
        types.add(aggregator.getAggregateOperation().getType());
        names.add(String.format("%s(%s)", aggregator.getAggregateOperation().toString(),
                aggregator.getOriginalTupleDesc().getFieldName(aggregator.getAggregateField())));

        this.tupleDesc = new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }

    @Override
    public void close() {
        super.close();
        this.groupingIterator = null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.groupingIterator = this.groupingFields.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.groupingIterator = this.groupingFields.iterator();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        Field groupingField;
        if (groupingIterator.hasNext()) {
            groupingField = groupingIterator.next();
        } else {
            return null;
        }

        Tuple tuple = new Tuple(getTupleDesc());
        int index = 0;

        // 分组查询
        if (aggregator.grouping()) {
            tuple.setField(index++, groupingField);
        }

        // 聚合查询
        tuple.setField(index++, this.getAggregatorField(aggregator.getAggregateOperation(), groupingField));

        return tuple;
    }

    protected abstract Field getAggregatorField(Aggregator.Op what, Field groupField);

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {

    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }
}
