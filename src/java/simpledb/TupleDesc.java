package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TupleDesc.TDItem> {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field.
         * */
        public final Type fieldType;
        
        /**
         * The name of the field.
         * */
        public final String fieldName;

        /**
         * Create new TDItem.
         * @param type The type of the field.
         * @param name	The name of the field.
         */
        public TDItem(Type type, String name) {
            this.fieldName = name;
            this.fieldType = type;
        }

        /**
         * @return String of the form: fieldName(fieldType).
         */
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * Collection (vector) to stores the Tuple Description Items.
     */
    private Vector<TDItem> m_tdItems;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc.
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return m_tdItems.iterator();
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
        // some code goes here
    	
    	// Assert the arrays are the same length
    	assert(typeAr.length == fieldAr.length) : 
    		"There are " + typeAr.length + " types and " + fieldAr.length + 
    		" names while constructing a TupleDesc.";
    	
    	m_tdItems = new Vector<TupleDesc.TDItem>(typeAr.length);
    	// For each type/field name, add a TDItem
    	for (int i=0;i<typeAr.length;i++) {
    		m_tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
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
        // some code goes here
    	
    	m_tdItems = new Vector<TupleDesc.TDItem>(typeAr.length);
    	for (Type t : typeAr) {
    		m_tdItems.add(new TDItem(t, null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return m_tdItems.size();
    }

    
    /**
     * Check to make sure element with index i exists within m_tdItems.
     * @param i The index
     * @throws NoSuchElementException if i >= m_tdItems.size()
     */
    private void checkElementExists(int i) throws NoSuchElementException {
    	if (i >= m_tdItems.size()) {
        	throw new NoSuchElementException("Tried to access field " + i + " but there are only " + 
        									 m_tdItems.size() + " fields.");
        } 
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
        // some code goes here
    	checkElementExists(i);
    	return m_tdItems.elementAt(i).fieldName;
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
        // some code goes here
    	checkElementExists(i);
    	return m_tdItems.elementAt(i).fieldType;        
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
        // some code goes here
        
    	for (int i=0;i<m_tdItems.size();i++) {
    		if (m_tdItems.elementAt(i).fieldName != null &&
    				m_tdItems.elementAt(i).fieldName.equals(name)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException("No element with name " + name + " exists.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int numBytes=0;
    	
    	for (TDItem td : m_tdItems) {
    		numBytes += td.fieldType.getLen();
    	}
        return numBytes;
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
        // some code goes here
    	
    	Vector<Type>   typeAr  = new Vector<Type>  (td1.getSize()+td2.getSize());
    	Vector<String> fieldAr = new Vector<String>(td1.getSize()+td2.getSize());
    	
    	for (TDItem item : td1) {
    		typeAr.add(item.fieldType);
    		fieldAr.add(item.fieldName);
    	}
    	
    	for (TDItem item : td2) {
    		typeAr.add(item.fieldType);
    		fieldAr.add(item.fieldName);
    	}
    	
        return new TupleDesc((Type[])   typeAr.toArray(new Type[typeAr.size()]), 
        		 			 					 (String[]) fieldAr.toArray(new String[fieldAr.size()]));
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
        // some code goes here        
    	if (o instanceof TupleDesc) {						      // IF the object is the correct type
    		TupleDesc other = (TupleDesc) o;
    			
    		if (other.getSize() == getSize()) {				  // And it has the same number of TupleDescs
    			Iterator<TDItem> it1 = other.iterator();	// Iterate over both collections
    			Iterator<TDItem> it2 = iterator();
    			    			
    			while (it1.hasNext()) {										// For each TDItem
    				TDItem item1 = it1.next();
    				TDItem item2 = it2.next();
    				
    				if (item1.fieldType != item2.fieldType) {	// If not the same
    					return false;														// Then not equal    			
    				}
    			}
    			return true;	// Same type, same size, all fields the same means equal
    		}    		
    	} 
    	
    	return false;		// If not write type or not same length, then not equal
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
        // some code goes here
    		StringBuffer sb = new StringBuffer();
    		
    		for (TDItem item : this) {
    			if (sb.length() > 0) {
    				sb.append(", ");
    			}
    			sb.append(item.fieldType.toString() + "(" + item.fieldName + ")");
    		}
        return sb.toString();
    }
}
