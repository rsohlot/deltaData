# deltaData

A wrapper over delta lake to provide some functionalities

## NOTE

* For now it is capable of reading the data from local delta lake only.

## TODO

```markdown
[ ] Add compability to store the data in s3.

```

## SETUP

```bash
python -m venv venv

# for windows , on terminal 
.\venv\Scripts\activate

# for linux based system
source venv/bin/activate

pip install -r requirements.txt
```

## USAGE

```python
    # create new object of the class deltaData
    from delta_data import deltaData

    data = deltaData()
    spark = data.get_spark_session()
    delta_data_path = "lake"

    # Read some data 
    df = spark.read.csv("data_path")
    
    # data is to partitionby
    partition_by_cols = ['id']

    # id columns in data
    id_cols = ['id']
    # add the data to lake
    data.smart_append(spark, df, delta_data_path, id_cols,partition_by_cols)

    # read one more data
    df2 = spark.read.csv("data_path_2")

    # add the 2nd data to lake
    data.smart_append(spark, df2, delta_data_path, id_cols, partition_by_cols)

    # compare the data versions
    data.compare_versions(spark, delta_data_path, id_cols)
```
