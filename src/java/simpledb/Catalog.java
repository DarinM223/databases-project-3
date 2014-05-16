package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	/**
	 * Class that creates a table used with mapping and
	 * need to make a container of all these tables which
	 * the catalog hold.
	 */
    public static class Table 
    {
            public String name;
            public String primaryKey;
            public DbFile file;


            public Table(String name, String primaryKey, DbFile file) {
                    this.name = name;
                    this.primaryKey = primaryKey;
                    this.file = file;
            }

    }
    
    Map<String, Integer> nameToId;
    Map<Integer, Table> idToTable;
    List<Integer> ids;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        idToTable = new ConcurrentHashMap<Integer, Table>();
        nameToId = new ConcurrentHashMap<String, Integer>();
        ids = new ArrayList<Integer>();
    }

   /**
    * Returns a list of tables in the Catalog
    */
    public List<Table> get_list_of_tables() {
        List<Table> list_of_tables = new ArrayList<Table>();

    	for(int i = 0; i < ids.size(); i++)
    	{
    		list_of_tables.add(idToTable.get(ids.get(i))); 
    	}
    	return list_of_tables;
    }
    
    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        idToTable.put(file.getId(), new Table(name, pkeyField, file));
        nameToId.put(name, file.getId());
        ids.add(file.getId());
    }
    
    /**
     * Add's a table into the catalog via mapping structures
     * @param file name of the file the table is being added to
     * @param name the name of the table
     */
    public void addTable(DbFile file, String name) {
    	//adds a table to the mapping structures
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Integer retValue;
        try {
                retValue = nameToId.get(name);
        } catch (NullPointerException e) {
                throw new NoSuchElementException();
        }
        if (retValue == null) 
                throw new NoSuchElementException();
        else 
                return retValue;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        TupleDesc retDesc;
        try {
                retDesc = idToTable.get(tableid).file.getTupleDesc();
        } catch (NullPointerException e) {
                throw new NoSuchElementException();
        }
        if (retDesc == null)
                throw new NoSuchElementException();
        else
                return retDesc;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile retFile;
        try {
                retFile = idToTable.get(tableid).file;
        } catch (NullPointerException e) {
                throw new NoSuchElementException();
        }
        if (retFile == null)
                throw new NoSuchElementException();
        else
                return retFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        return idToTable.get(tableid).primaryKey;
    }

    /**
     *  Iterate through all the tables created in the list
     */
    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return ids.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        return idToTable.get(id).name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        nameToId.clear();
        idToTable.clear();
        ids.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

