package simpledb.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.io.Serial;
import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregate.class);

    @Serial
    private static final long serialVersionUID = -9135544931049615936L;

    private final OpIterator child;
    private OpIterator aggregatorIterator;

    private final int aggregateField;
    private final int groupField;
    private final Aggregator.Op aggregateOperation;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of aggregateField, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child              The OpIterator that is feeding us tuples.
     * @param aggregateField     The column over which we are computing an aggregate.
     * @param groupField         The column over which we are grouping the result, or -1 if there is no grouping
     * @param aggregateOperation The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aggregateField, int groupField, Aggregator.Op aggregateOperation) {
        this.child = child;
        this.aggregateField = aggregateField;
        this.groupField = groupField;
        this.aggregateOperation = aggregateOperation;
        this.initAggregatorIterator();
    }

    private void initAggregatorIterator() {
        // 根据聚合查询的字段类型，初始化对应的Aggregator类
        Type aggregateFieldType = child.getTupleDesc().getFieldType(aggregateField);
        Type groupFieldType;
        if (this.groupField >= 0) {
            groupFieldType = child.getTupleDesc().getFieldType(groupField);
        } else {
            // 不是分组查询时，groupFieldType设置为null
            groupFieldType = null;
        }

        Aggregator aggregator;
        if (aggregateFieldType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupField, groupFieldType, aggregateField, aggregateOperation);
        } else if (aggregateFieldType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(groupField, groupFieldType, aggregateField, aggregateOperation);
        } else {
            throw new UnsupportedOperationException("unsupported aggregate type: " + aggregateFieldType);
        }
        // 给这个Aggregator填充数据
        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
            child.close();
        } catch (Exception e) {
            LOGGER.error("initialize Aggregator error.", e);
        }
        this.aggregatorIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return this.child.getTupleDesc().getFieldName(groupField());
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(aggregateField());
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aggregateOperation;
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();
        this.aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggregatorIterator.hasNext()) {
            return aggregatorIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return this.aggregatorIterator.getTupleDesc();
    }

    public void close() {
        super.close();
        this.aggregatorIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

}
