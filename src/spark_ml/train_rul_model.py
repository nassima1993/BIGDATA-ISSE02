import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


def main():
    spark = (
        SparkSession.builder.appName("ISSE02-Train-RUL-Model")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(">>> Session Spark (Entraînement RUL) démarrée.")

    # Chemin aligné avec la structure du dépôt BIGDATA-ISSE02
    data_path = (
        "data/raw/nasa_cmapps/train_FD001.txt"
        if os.path.exists("data/raw/nasa_cmapps/train_FD001.txt")
        else "data/train_FD001.txt"
    )
    raw_df = spark.read.csv(data_path, sep=" ", inferSchema=True)
    df = raw_df.select(*raw_df.columns[:26])

    # Noms de colonnes alignés avec producer.py
    col_names = (
        ["engine_id", "cycle"]
        + [f"setting_{i}" for i in range(1, 4)]
        + [f"sensor_{i}" for i in range(1, 22)]
    )
    df = df.toDF(*col_names)

    # Calcul de la RUL avec plafonnement industriel à 125 cycles
    max_cycles = df.groupBy("engine_id").agg(F.max("cycle").alias("max_cycle"))
    df = (
        df.join(max_cycles, on="engine_id", how="inner")
        .withColumn("RUL", F.col("max_cycle") - F.col("cycle"))
        .drop("max_cycle")
        .withColumn("RUL", F.when(F.col("RUL") > 125, 125).otherwise(F.col("RUL")))
    )

    # Feature Engineering Spark SQL : moyennes glissantes sur 5 cycles
    df.createOrReplaceTempView("capteurs_view")
    capteurs_utiles = [
        "sensor_2", "sensor_3", "sensor_4", "sensor_7", "sensor_8", "sensor_9",
        "sensor_11", "sensor_12", "sensor_13", "sensor_14", "sensor_15",
        "sensor_17", "sensor_20", "sensor_21",
    ]
    rolling_exprs = [
        f"AVG({c}) OVER (PARTITION BY engine_id ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS {c}_moy_5c"
        for c in capteurs_utiles
    ]
    df_features = spark.sql(f"SELECT *, {', '.join(rolling_exprs)} FROM capteurs_view")

    # Entraînement GBTRegressor
    feature_cols = capteurs_utiles + [f"{c}_moy_5c" for c in capteurs_utiles]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

    gbt = GBTRegressor(featuresCol="features", labelCol="RUL", maxIter=50, maxDepth=5, seed=42)
    model = Pipeline(stages=[assembler, gbt]).fit(train_df)

    # Évaluation RMSE
    predictions = model.transform(test_df)
    rmse = RegressionEvaluator(labelCol="RUL", predictionCol="prediction", metricName="rmse").evaluate(predictions)
    print(f">>> Performance du modèle GBTRegressor — RMSE Test : {rmse:.2f} cycles")

    try:
        model.write().overwrite().save("src/spark_ml/gbt_rul_model")
        print(">>> Modèle sauvegardé dans 'src/spark_ml/gbt_rul_model'.")
    except Exception:
        print(">>> Note Windows : Sauvegarde Hadoop ignorée localement (active sous Docker/Linux).")

    spark.stop()


if __name__ == "__main__":
    main()