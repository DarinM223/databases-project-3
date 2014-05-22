package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int store[];
    private int buckets;
    private int min;
    private int max;
    private int width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.store = new int[buckets];
        this.min = min;
        this.max = max;
        Arrays.fill(this.store, 0);
        this.width = (int) Math.ceil((double) (this.max - this.min + 1) / this.buckets);
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        for (int i = 0; i < store.length; i++) {
                int bucketMin = min + this.width*i;
                int bucketMax = min + this.width*(i+1);
                if (v >= bucketMin && v < bucketMax) {
                        this.store[i]++;
                        this.ntups++;
                        break;
                }
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        //return -1.0;
        double retVal = 0.0;
        String opString;
        switch (op) {
                case EQUALS:
                        retVal = eqSelectivity(v);
                        opString = "EQUALS TO";
                        break;
                case GREATER_THAN:
                        retVal = grSelectivity(v);
                        opString = "GREATER THAN";
                        break;
                case GREATER_THAN_OR_EQ:
                        retVal = grEqSelectivity(v);
                        opString = "GREATER THAN OR EQUAL TO";
                        break;
                case LESS_THAN:
                        retVal = leSelectivity(v);
                        opString = "LESS THAN";
                        break;
                case LESS_THAN_OR_EQ:
                        retVal = leEqSelectivity(v);
                        opString = "LESS THAN OR EQUAL TO";
                        break;
                case NOT_EQUALS:
                        retVal = 1 - eqSelectivity(v);
                        opString = "NOT EQUAL TO";
                        break;
                default:
                        opString = "";
                        break;
        }
        System.out.println("Selectivity of " + opString + " " + v + " is " + retVal);
        return retVal;
    }
    
    public double eqSelectivity(int v) {
            for (int i = 0; i < store.length; i++) {
                  int bucketMin = min + this.width*i;
                  int bucketMax = min + this.width*(i+1);
                  if (v >= bucketMin && v < bucketMax) {
                          int height = store[i];
                          return ((double)height/width) / ntups;
                  }
            }
            return 0;
    }
    public double leSelectivity(int v) {
            if (v < min) { return 1; }
            if (v >= max) { return 0; }
            double totalSelectivity = 0;
            int found = Integer.MAX_VALUE;
            for (int i = 0; i < store.length; i++) {
                    int bucketMin = min + this.width*i;
                    int bucketMax = min + this.width*(i+1);
                    int height = store[i];
                    double b_f = (double)height / ntups;
                    if (v >= bucketMin && v < bucketMax) {
                            found = i;
                            double b_part = ((double)v - bucketMin) / width;
                            double partSelectivity = b_f * b_part;
                            totalSelectivity += partSelectivity;
                    }
                    if (i < found) {
                            totalSelectivity += b_f;
                    }
            }
            System.out.println(totalSelectivity);
            return totalSelectivity;
    }
    public double leEqSelectivity(int v) {
            if (v < min) { return 1; }
            if (v >= max) { return 0; }
            double totalSelectivity = 0;
            int found = Integer.MAX_VALUE;
            for (int i = 0; i < store.length; i++) {
                    int bucketMin = min + this.width*i;
                    int bucketMax = min + this.width*(i+1);
                    int height = store[i];
                    if (v >= bucketMin && v < bucketMax) {
                            found = i;
                    }
                    if (i < found) {
                            double b_f = (double)height / ntups;
                            totalSelectivity += b_f;
                    }
            }
            return totalSelectivity;
    }
    public double grSelectivity(int v) {
            if (v < min) { return 1; }
            if (v >= max) { return 0; }
            double totalSelectivity = 0;
            int found = Integer.MAX_VALUE;
            for (int i = 0; i < store.length; i++) {
                    int bucketMin = min + this.width*i;
                    int bucketMax = min + this.width*(i+1);
                    int height = store[i];
                    double b_f = (double)height / ntups;
                    if (i > found) {
                            totalSelectivity += b_f;
                    }
                    if (v >= bucketMin && v < bucketMax) {
                            found = i;
                            double b_part = ((double)bucketMax - v) / width;
                            double partSelectivity = b_f * b_part;
                            totalSelectivity += partSelectivity;
                    }
            }
            return totalSelectivity;
    }
    public double grEqSelectivity(int v) {
            if (v < min) { return 1; }
            if (v >= max) { return 0; }
            double totalSelectivity = 0;
            int found = Integer.MAX_VALUE;
            for (int i = 0; i < store.length; i++) {
                    int bucketMin = min + this.width*i;
                    int bucketMax = min + this.width*(i+1);
                    int height = store[i];
                    if (i > found) {
                            double b_f = (double)height / ntups;
                            totalSelectivity += b_f;
                    }
                    if (v >= bucketMin && v < bucketMax) {
                            found = i;
                    }
            }
            return totalSelectivity;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
