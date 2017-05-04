package org.apache.graphframe.gfjex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;

import org.graphframes.*;
import org.graphframes.examples.*;


// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Mirko Kämpf
 *
 * The GraphFramesAppStub is a generic template for Graphfraes applications in
 * Java. It allows a quicker start of experiments.
 *
 */
public class GraphFramesAppStub {

    public static void main(String[] args) {

        String appName = "GFApp.1";
        String[] master = {"local[1]"};

        System.out.println("GFApp is generating a   S Q L C o n t e x t  ...");
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master[0]);
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        //======================================================================
        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> people = sc.textFile("src/main/resources/people.txt");
        System.out.println( "# Nr of distinc people records: " + people.distinct().count() );
        
        // The schema is encoded in a string
        String schemaString = "id name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = people.map(
                new Function<String,Row>() {
            public Row call(String record) throws Exception {
                String[] fields = record.split(",");
                //System.out.println("{"+record+"}:" + fields.length);
                return RowFactory.create(fields[0],fields[1],fields[2]);
            }
        });
      
        // Apply the schema to the RDD.
        DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

        // Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame peopleDF = sqlContext.sql("SELECT name, id FROM people");

        System.out.println( "# Nr of distinc people records from DF: " + peopleDF.toJavaRDD().count() );
               
         
        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = peopleDF.javaRDD().map(new Function<Row,String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0) + "(id:" + row.getString(1) + ")";
            }
        }).collect();
        
        Iterator<String> namesList = names.iterator();
        
        while( namesList.hasNext() ) {
            String n = namesList.next();
            //System.out.println( "* " + n );
        }
       
        //======================================================================
        
        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> links = sc.textFile("src/main/resources/links.txt");
        System.out.println( "# Nr of distinc links records: " + links.count() );
        
        // The schema is encoded in a string
        String schemaStringLinks = "src dst weight";

        // Generate the schema based on the string of schema
        List<StructField> fieldsLinks = new ArrayList<StructField>();
        for (String fieldName : schemaStringLinks.split(" ")) {
            fieldsLinks.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schemaLinks = DataTypes.createStructType(fieldsLinks);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDDLinks = links.map(
                new Function<String,Row>() {
            public Row call(String record) throws Exception {
                String[] fields = record.split(",");
                //System.out.println(fields[0]);
                return RowFactory.create(fields[0],fields[1].trim(),fields[2].trim());
            }
        });
        
        DataFrame linksDF = sqlContext.createDataFrame( rowRDDLinks, schemaLinks );
        
        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> linksLabel = linksDF.javaRDD().map(new Function<Row,String>() {
            public String call(Row row) {
                return "<from>" + row.getString(0) + "</from><to>" + row.getString(1) + "</to><weight>"  + row.getString(2) + "</weight>\n";
            }
        }).collect();
        
        Iterator<String> linksList = linksLabel.iterator();
        
        while( linksList.hasNext() ) {
            String n = linksList.next();
            //System.out.println( n );
        }
        
        //======================================================================
   
        GraphFrame gf = GraphFrame.apply(peopleDF, linksDF);
        
        //======================================================================
   
        DataFrame e = gf.edges();
        DataFrame v = gf.vertices();
        DataFrame degree = gf.degrees();
        DataFrame triplets = gf.triplets();
        
        //======================================================================
        
        dumpDF( e , "edges" );
        dumpDF( v , "vertice" );
        dumpDF( degree , "degree" );
        dumpDF( triplets , "triplets" );
        
        //======================================================================
        sc.close();
        System.out.println("GFApp destroys the   J a v a S p a r k C o n t e x t.");

    }
    
    /**
     * Store a DataFrame as Parquet file in a temp folder with one partition.
     * 
     * @param df
     * @param label 
     */
    public static void dumpDF( DataFrame df, String label) {
        
        long ts = System.currentTimeMillis();
        
        List<String> data = df.javaRDD().map(new Function<Row,String>() {
            public String call(Row row) {
                StringBuffer sb = new StringBuffer();
                int i = 0;
                while( i < row.length() ) {
                    if ( i > 0 ) sb.append(",");
                    sb.append( row.get(i));
                    i++;
                };
                return sb.toString();
            }
        }).collect();

        Iterator<String> list = data.iterator();
        
        System.out.println( "======================================================================" );
        System.out.println( "DataFrame: " + label );
        System.out.println( "======================================================================" );
        
        while( list.hasNext() ) {
            String n = list.next();
            System.out.println( n.substring(0, n.length()) );
        }
        
        System.out.println( "======================================================================" );
        
        df.coalesce(1).write().parquet( "temp/" + label + "-" + ts + ".parquet");
       
        System.out.println( "Path: " + "temp/" + label + "-" + ts + ".parquet" );
        System.out.println( "======================================================================\n\n" );
        
    };
    

}
