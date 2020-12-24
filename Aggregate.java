package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // declare fields
    private DbIterator node, aggrNode;
    int af, gf;
    Aggregator.Op operation;
    private Aggregator aggregator;
    private Iterator<Tuple> tupIter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.node = child;
        this.af = afield;
        this.gf = gfield;
        this.operation = aop;

        Type gbFieldType, aFieldType = node.getTupleDesc().getFieldType(af);

        if (gf == Aggregator.NO_GROUPING){
            gbFieldType = null;
        }
        else{
            gbFieldType = node.getTupleDesc().getFieldType(gf);
        }

        if (aFieldType == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gf, gbFieldType, af, operation);
        }
        else{
            aggregator = new StringAggregator(gf, gbFieldType, af, operation);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return gf;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(gf== Aggregator.NO_GROUPING){
            return null;
        }
        return node.getTupleDesc().getFieldName(gf);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return af;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        String res = node.getTupleDesc().getFieldName(af);
        return res;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return operation;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        node.open();
        while (node.hasNext() != false){
            Tuple nextNode = node.next();
            aggregator.mergeTupleIntoGroup(nextNode);
        }
        aggrNode = aggregator.iterator();
        aggrNode.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggrNode.hasNext() != false){
            Tuple nextTup = aggrNode.next();
            return nextTup;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        node.rewind();
        aggrNode.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {

        Type[] typeArray;
        String[] stringArray;

        if (af == Aggregator.NO_GROUPING){
            typeArray = new Type[1];
            typeArray[0] = node.getTupleDesc().getFieldType(af);
            stringArray = new String[1];
            stringArray[0] = operation.toString() + "(" + typeArray[0] + ")";

            return new TupleDesc(typeArray, stringArray);
        }
        else{
            typeArray = new Type[2];
            stringArray = new String[2];

            typeArray[0] = node.getTupleDesc().getFieldType(gf);
            stringArray[0] = node.getTupleDesc().getFieldName(gf);

            typeArray[1] = node.getTupleDesc().getFieldType(af);
            stringArray[1] = operation.toString() + "(" + node.getTupleDesc().getFieldName(af) + ")";

            return new TupleDesc(typeArray, stringArray);
        }
    }

    public void close() {

        node.close();
        aggrNode.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {

        return new DbIterator[]{this.node};
    }

    @Override
    public void setChildren(DbIterator[] children) {

        if (this.node != children[0]){
            this.node = children[0];
        }
    }
}
