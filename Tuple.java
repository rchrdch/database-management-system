package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    // additional fields
    private TupleDesc tupleDesc;
    private RecordId recordId;
    ArrayList<Field> tuple;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // error check
        if(!(td instanceof TupleDesc)){
            throw new IllegalArgumentException("td must be a valid TupleDesc instance with at least one field");
        }

        tupleDesc = td;
        tuple = new ArrayList<Field>();
        // fill with placeholders...
        for(int i = 0; i<td.numFields(); i++){
            tuple.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        this.tuple.add(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // error check
        //System.out.println("the i being called by getField is "+i);
        if(i<0 || i>(this.tuple.size()-1)){
            throw new IllegalArgumentException("i must be a valid index");
        }

        return this.tuple.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // initialize string to be returned
        String ret = "";
        int size = this.tuple.size() - 1;

        if (this.tuple.size() <= 0){
            return ret;
        }

        for (int i = 0; i < size; i++){
            ret.concat(this.tuple.get(i).toString());
            ret.concat(" ");
        }
        //throw new UnsupportedOperationException("Implement this");
        return ret;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return this.tuple.iterator();
    }

    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
    }

    /**
     * merge -- merges two tuples into one
     * helpful for PA1.3 (Join file)
      */
    public static Tuple merge(Tuple tup1, Tuple tup2){
        TupleDesc res = TupleDesc.merge(tup1.getTupleDesc(), tup2.getTupleDesc());
        Tuple newTup = new Tuple(res);

        // set all the new fields from Tuple 1
        int numFields1 = tup1.getTupleDesc().numFields();
        for(int i = 0; i < numFields1; i++){
            Field tup1Field = tup1.getField(i);
            newTup.setField(i, tup1Field);
        }

        int offset = tup1.getTupleDesc().numFields();

        // set all the new fields from Tuple 2
        int numFields2 = tup2.getTupleDesc().numFields();
        for(int i = 0; i < numFields2; i++){
            Field tup2Field = tup2.getField(i);
            newTup.setField(offset+i, tup2Field);
        }
        return newTup;
    }
}
