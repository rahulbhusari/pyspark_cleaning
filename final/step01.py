from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.master("local").getOrCreate()

cleaned_titles_df = spark.read.csv(
    path="./data/final_cleaned_netflix_titles_data.csv", header=True, sep="\t"
)

cast_members_df = cleaned_titles_df.withColumn(
    "cast_count", F.size(F.split(cleaned_titles_df.cast, ","))
)
cast_members_df.select("title", "cast_count").show(50, truncate=False)

"""
+----------------------------------------+----------+
|title                                   |cast_count|
+----------------------------------------+----------+
|D.L. Hughley: Clear                     |1         |
|My Scientology Movie                    |1         |
|Tom Segura: Completely Normal           |1         |
|4L                                      |8         |
|Age Gap Love                            |1         |
|Ainsley Eats the Streets                |1         |
|Black & Privileged: Volume 1            |9         |
|Blown Away                              |-1        |
|Cities of Last Things                   |5         |
|Encounters with Evil                    |1         |
|Extreme Engagement                      |2         |
|History's Greatest Hoaxes               |-1        |
|Kidnapping Stella                       |3         |
|Luis Miguel - The Series                |10        |
|Mega Food                               |-1        |
|MegaTruckers                            |-1        |
|Money for Nothing                       |-1        |
|One Spring Night                        |8         |
|PILI Fantasy: War of Dragons            |10        |
|Point Blank                             |7         |
|Smart People                            |10        |
|Taco Chronicles                         |-1        |
|The Milk System                         |-1        |
|True Tunes                              |7         |
|Gonul                                   |3         |
|Chhota Bheem                            |7         |
|Family Reunion                          |8         |
|Little Singham Bandarpur Mein Hu Ha Hu  |10        |
|Parchís: the Documentary                |-1        |
|Us and Them                             |8         |
|The Battered Bastards of Baseball       |2         |
|Sex Doll                                |7         |
|Summer of '92                           |10        |
|Chris Tucker Live                       |1         |
|Alice Doesn't Live Here Anymore         |10        |
|DreamWorks Kung Fu Panda Awesome Secrets|11        |
|Flowering Heart                         |6         |
|Frozen River                            |10        |
|Inkheart                                |10        |
|Inside the Mind of a Serial Killer      |-1        |
|Katherine Ryan: Glitter Room            |1         |
|Kill the Irishman                       |19        |
|Lady in the Water                       |10        |
|Little Monsters                         |12        |
|Mean Dreams                             |8         |
|Mean Streets                            |8         |
|Molang                                  |1         |
|Next                                    |10        |
|Nights in Rodanthe                      |10        |
|NOVA: Bird Brain                        |1         |
+----------------------------------------+----------+
only showing top 50 rows

"""
