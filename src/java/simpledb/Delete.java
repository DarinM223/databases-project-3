package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */

public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId transId;
    private DbIterator child;
    
    private List<Tuple> tuples; 

    private BufferPool pool = Database.getBufferPool();

    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.transId = t;
    	this.child = child;
    	this.tuples = new LinkedList<Tuple>();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeAr = { Type.INT_TYPE };
        return new TupleDesc(typeAr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();

    	int tupleCount = 0;
    	//keep a list of the child operators
    	if(child.hasNext())
    	{
    		//add them in order of query processing
    		List<Tuple> tupleList = new LinkedList<Tuple>();
    		while(child.hasNext())
    		{
    			tupleList.add(child.next());
    			tupleCount++;
    		}

    		try
    		{
    			//delete the tuples in cases of join
    			for(Tuple t : tupleList)
    			{
    				pool.deleteTuple(transId, t);
    			}
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	}
    	Tuple t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(tupleCount));
    	tuples.add(t);
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
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
        // some code goes here
    	//if there are more tuples
        if(!tuples.isEmpty())
        {
        	//remove the one of the tuples
        	return tuples.remove(0);
        }
	else
		return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	// some code goes here
    	child = children[0];
    }
    
}

