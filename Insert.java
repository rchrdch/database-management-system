package simpledb;

import javax.swing.plaf.metal.MetalBorders;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator node;
    private int tabId;
    private boolean access = false;
    private TupleDesc tupD;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.node = child;
        this.tabId = tableId;
        Type[] typeArray = new Type[1];
        String[] stringArray = new String[1];
        typeArray[0] = Type.INT_TYPE;
        stringArray[0] = "Num. of records";
        this.tupD = new TupleDesc(typeArray, stringArray);

    }

    public TupleDesc getTupleDesc() {
        return this.tupD;
    }

    public void open() throws DbException, TransactionAbortedException {
        node.open();
        super.open();
    }

    public void close() {
        node.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        node.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t = new Tuple(this.tupD);
        // counter variable
        int num = 0;
        if (access == true){
            return null;
        }
        else if (access == false){
            access = true;
            while (node.hasNext() == true){
                Tuple nextTup = node.next();
                try{
                    Database.getBufferPool().insertTuple(tid, tabId, nextTup);
                }
                catch (IOException e){
                    e.printStackTrace();
                }
                num += 1;
            }

            Field f = new IntField(num);
            t.setField(0, f);
        }
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.node};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.node = children[0];
    }
}

