package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    // Initialize fields
    private Predicate pr;
    private DbIterator chOp;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.pr = p;
        this.chOp = child;
    }

    public Predicate getPredicate() {
        return this.pr;
    }

    public TupleDesc getTupleDesc() {
        TupleDesc result = this.chOp.getTupleDesc();
        return result;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.chOp.open();
        super.open();
    }

    public void close() {
        super.close();
        this.chOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.chOp.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(this.chOp.hasNext()){
            Tuple tup = this.chOp.next();
            Boolean result = this.pr.filter(tup);
            if (result) {
            return tup;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.chOp};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(this.chOp!=children[0]){
            this.chOp = children[0];
        }
    }

}
