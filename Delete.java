package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator node;
    private TupleDesc tupD;
    private boolean access = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.node = child;
        Type[] typeArray = new Type[1];
        String[] stringArray = new String[1];
        typeArray[0] = Type.INT_TYPE;
        stringArray[0] = "Num of deletions";
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t = new Tuple(tupD);
        int num = 0;
        if(access == true){
            return null;
        }
        else if (access == false){
            access = true;
            while(node.hasNext() == true){
                Tuple nextTup = node.next();
                try{
                    Database.getBufferPool().deleteTuple(tid, nextTup);}
                catch (IOException e){
                    e.printStackTrace();
                }
                num++;
            }
            Field f = new IntField(num);
            t.setField(0, f);
        }
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {this.node};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.node = children[0];
    }

}
