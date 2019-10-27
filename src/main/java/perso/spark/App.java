package perso.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // creation de l'objet sparksession...
        SparkSession spark = SparkSession.builder().master("local[*]").appName("spark_test ").getOrCreate();

        //READ THE DATA FROM CSV.
        Dataset<Row> csv = spark.read().format("csv").option("header","true").option("delimiter", ";").load("/home/work/Documents/SimBEL/mp_epeuro1.csv");

        //Pas de doublon de colonnes, possibilité de réaffecter la valeur d'une colonne déjà créer.
        csv = csv.withColumn("montant_rachat", functions.col("pm_gar").plus(functions.col("chgt_rach")));
        csv = csv.withColumn("montant_rachat", functions.col("pm_gar").multiply(0));

        //Concatener deux colonnes
        csv = csv.withColumn("concat_test", functions.concat(functions.col("num_rach_dyn_tot"),functions.lit(" _-_ "),functions.col("type_cot")) );

        csv.show();
        //System.out.println("Hello World!");

        spark.stop();
    }
}