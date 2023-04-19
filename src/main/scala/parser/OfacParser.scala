package parser

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OfacParser extends App {

  val spark = SparkSession
    .builder()
    .appName("Dataframe Operations")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val xmlPath = "src/resources/sdn_advanced_2.xml"

  val xmlDf = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "Sanctions")
    .load(xmlPath)


  //  _________
  //  | ALIAS |
  //  ---------

  val profileDf = xmlDf
    .select(explode(col("DistinctParties.DistinctParty")).as("DistinctParty"))
    .select("DistinctParty.Profile")

  val aliasDf = profileDf
    .select(explode(col("Profile.Identity.Alias")).as("Alias"), col("Profile._ID").as("ProfileID"))
    .select(col("Alias.*"), col("ProfileID"))
    .withColumn("DocumentedName", explode(col("DocumentedName")))
    .withColumn("DocumentedNamePart", explode(col("DocumentedName.DocumentedNamePart")))
    .select("_FixedRef", "_AliasTypeID", "DocumentedNamePart.NamePartValue._VALUE", "ProfileID").groupBy("ProfileID", "_FixedRef", "_AliasTypeID")
    .agg(collect_list("_VALUE").as("Alias_List"))

  val aliasReferenceDf = xmlDf
    .select("ReferenceValueSets.*")
    .select(explode(col("AliasTypeValues.AliasType")).as("AliasType"))
    .select(col("AliasType._ID").as("_AliasTypeID"), col("AliasType._VALUE").as("AliasTypeValue"))

  val aliasData = aliasReferenceDf.join(aliasDf, "_AliasTypeID")

  val refinedAliasData =
    aliasData
      .withColumn("map", map(col("AliasTypeValue"), col("Alias_List")))
      .groupBy("ProfileID", "_FixedRef")
      .agg(collect_list("map")).as[(String, String, Seq[Map[String, Seq[String]]])]
      .map { case (id, fixedRef, list) =>
        val x = list.reduce(_ ++ _)
        (id, fixedRef, x.getOrElse("Name", Nil).mkString(","), x.getOrElse("A.K.A.", Nil).mkString(";"))
      }.toDF("ProfileID", "FixedRef", "Name", "Alias")

  val featureDf = profileDf
    .select(explode(col("Profile.Feature")).as("Feature"), col("Profile._ID").as("ProfileID"))
    .select(col("Feature.*"), col("ProfileID"))
    .where(col("_FeatureTypeID") === 25)
    .select(col("FeatureVersion.VersionLocation._LocationID").as("LocationID"), col("FeatureVersion._ID").as("FeatureVersionID"), col("ProfileID"))
    .withColumn("rank", rank().over(Window.partitionBy("ProfileID").orderBy(desc("FeatureVersionID"))))
    .filter(col("rank") === 1).drop("rank", "FeatureVersionID")

  val locationDf = refinedAliasData.join(featureDf, "ProfileID")

  //  ___________
  //  | ADDRESS |
  //  -----------

  val addressDf = xmlDf
    .select(explode(col("Locations.Location")).as("Location"))
    .select("Location.*")
    .withColumn("LocationPart", explode(col("LocationPart")))
    .withColumn("LocationCountry", col("LocationCountry").as("LocationCountry"))
    .withColumn("LocationPartValue", explode(col("LocationPart.LocationPartValue")))
    .select(col("LocationPartValue.Value"), col("_ID").as("LocationID"), col("LocationCountry._CountryID"))
    .groupBy("LocationID", "_CountryID").agg(collect_list("Value").as("Address"))
    .withColumn("Address", array_join(col("Address"), ","))

  val addressReferenceDf = xmlDf
    .select("ReferenceValueSets.*")
    .select(explode(col("AreaCodeValues.AreaCode")).as("AreaCode"))
    .select(col("AreaCode._CountryID"), col("AreaCode._Description").as("Country"))

  val addressData = addressDf.join(addressReferenceDf, "_CountryID")

  val finalDf = locationDf.join(addressData, "LocationID").select(col("Name"), col("Alias"), col("Country"), col("Address"))

  val data = finalDf
    .withColumn("WATCHLIST_ID", substring(rand(), 3, 4).cast("bigint"))
    .withColumn("NAME_OF_WATCHLIST", lit("sandbox_watchlist"))
    .withColumn("WATCHLIST_NAME", col("Name"))
    .withColumn("FIRST_NAME", split(col("Name"), ",")(1))
    .withColumn("LAST_NAME", split(col("Name"), ",")(0))
    .withColumn("WATCHLIST_ALIAS", col("Alias"))
    .withColumn("WATCHPERSON_RESIDENTIAL_ADDRESS", col("Address"))
    .withColumn("WATCHLIST_COUNTRY_OF_BIRTH", col("Country"))
    .withColumn("WATCHLIST_NATIONALITY", col("Country"))
    .withColumn("TT_IS_LATEST_DATA", lit("true"))
    .withColumn("TT_IS_DELETED", lit("FALSE"))
    .withColumn("TT_UPDATED_TIME", current_timestamp())
    .drop("Name", "Alias", "Address", "Country")

  data.show(10, false)

  data
    .coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep", "|")
    .save("src/resources/output_data/mydata.csv")

}
