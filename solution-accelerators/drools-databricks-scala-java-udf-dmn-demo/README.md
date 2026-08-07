# drools-databricks-dmn-map-udf-demo

This version uses the map-in / map-out UDF pattern.

## Flow

```text
DataFrame row
  -> Spark SQL map('DMN Input Name', column_value)
  -> Java UDF drools_score(input_map)
  -> Java DMN engine evaluates rules
  -> Java UDF returns output map
  -> Python reads _score_map and adds output columns
  -> Drop _score_map
  -> Write Delta table
```

## Important notebook code

```python
map_pairs = ", ".join(
    f"'{input_name}', cast({matched_inputs[input_name]} as string)"
    if matched_inputs[input_name] is not None
    else f"'{input_name}', cast(null as string)"
    for input_name in dmn_input_names
)

score_map_expr = f"drools_score(map({map_pairs}))"
scored = df.withColumn("_score_map", F.expr(score_map_expr))

result_df = scored
for decision_name in dmn_decision_names:
    result_df = result_df.withColumn(
        to_snake(decision_name),
        F.col("_score_map").getItem(decision_name).cast(spark_type)
    )

result_df = result_df.drop("_score_map")
result_df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
```

## Setup

### 1. Create input table

```sql
CREATE TABLE IF NOT EXISTS default.customer_dmn_input (
  customer_id INT,
  credit_score INT,
  income DOUBLE,
  state STRING,
  customer_segment STRING
);

INSERT INTO default.customer_dmn_input VALUES
(1, 620, 25000, 'NC', 'STANDARD'),
(2, 720, 90000, 'FL', 'PREMIUM'),
(3, 640, 60000, 'GA', 'STANDARD'),
(4, 580, 15000, 'NY', 'STANDARD'),
(5, 665, 42000, 'FL', 'PREMIUM');
```

### 2. Build and copy JAR

```bash
python scripts/build_and_copy_jar.py
```

### 3. Copy sample DMN to Volume

```bash
python scripts/copy_sample_dmn_to_volume.py
```
### 4. Update databricks.yml

Set:
- `workspace.host`
- `single_user_name`
- volume paths if needed

### 5. Deploy and run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run drools_dmn_map_udf_poc_job
```

### 6. Query output

```sql
SELECT *
FROM default.customer_dmn_map_udf_output;
```

## Features

- Reads DMN XML from UC Volume
- Scala parses DMN input/decision metadata
- Python matches DMN inputs to table columns
- Unmatched inputs are passed as null
- Java UDF returns a map
- Python expands `_score_map`
- FEEL-to-Spark output casting
- Row limit and repartition variables
