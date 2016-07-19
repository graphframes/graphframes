package org.apache.graphframe.gfjex;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

/**
 * @author Mirko Kämpf
 * 
 * The SparkAppStub is a generic template for Spark Applications in Java
 * and allows a quicker start of experiments.
 * 
 */
public class SparkAppStub {
    
    public static void main(String[] args) {
        
        String appName = "SparkApp.1";
        String[] master = {"local[4]"};
        
        System.out.println( "SparkApp is generating a   J a v a S p a r k C o n t e x t  ...");
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master[0]);
        JavaSparkContext sc = new JavaSparkContext(conf);

        //======================================================================

        


        //======================================================================

        sc.close();
        System.out.println( "SparkApp destroys the   J a v a S p a r k C o n t e x t.");
        
    }
    
}
