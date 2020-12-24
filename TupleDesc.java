package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    // ArrayList representing collection of fields
    private ArrayList<TDItem> collection;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */

    // ArrayList --> ListIterator
    public Iterator<TDItem> iterator() {
        return collection.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // error check
        if(typeAr.length < 1){
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }
        // initialize collection
        collection = new ArrayList<TDItem>();
        // add each field (TDItem) to collection
        for(int i = 0; i<typeAr.length; i++){
            TDItem field = new TDItem(typeAr[i], fieldAr[i]);
            collection.add(field);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // error check
        if(typeAr.length < 1){
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }
        // initialize collection
        collection = new ArrayList<TDItem>();
        // add each field (TDItem) to collection
        for(int i = 0; i<typeAr.length; i++){
            TDItem field = new TDItem(typeAr[i], "");
            collection.add(field);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return collection.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // error check
        int numFields = numFields()-1;
        if(i<0 || i>numFields){
            throw new NoSuchElementException("i is not a valid field reference");
        }
        // get name of field at position i
        String fieldName = collection.get(i).fieldName;
        return fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // error check
        int numFields = numFields()-1;
        if(i<0 || i>numFields){
            throw new NoSuchElementException("i is not a valid field reference");
        }
        // get name of field at position i
        Type fieldType = collection.get(i).fieldType;
        return fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int numFields = numFields()-1;
        for (int i = 0; i<= numFields; i++){
            if (collection.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // initialize size at 0
        int size = 0;
        //int numFields = numFields()-1;
        for (int i = 0; i< numFields(); i++) {
            Type currentField = collection.get(i).fieldType;
            size += currentField.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // initialize new array holding field types for td1 and td2
        int typeAr_size = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[typeAr_size];
        String[] fieldAr = new String[typeAr_size];

        for(int i = 0; i<td1.numFields(); i++){
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }

        // i continues at position where we left off from td1
        // j begins at 0, since it will be keeping track of our position on td2
        for(int i = td1.numFields(), j = 0; i<typeAr_size; i++, j++){
            typeAr[i] = td2.getFieldType(j);
            fieldAr[i] = td2.getFieldName(j);
        }

        // initialize newTD with new typeAr and fieldAr
        TupleDesc newTD = new TupleDesc(typeAr, fieldAr);
        return newTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null){
            return false;
        }

        if (!o.getClass().equals(this.getClass())){
            return false;
        }

        TupleDesc object = ((TupleDesc) o);

        if (object.numFields() != this.numFields()){
            return false;
        }

        int size = this.collection.size();

        for (int i = 0; i < size; i++) {
            if (!this.collection.get(i).fieldType.equals(object.collection.get(i).fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String ret = "";

        for (int i = 0; i < this.getSize(); i++){
            ret.concat(collection.get(i).fieldType.toString());
            ret.concat(collection.get(i).fieldName.toString());
            ret.concat(", ");
        }
        return ret;
    }
}
