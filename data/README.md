## Data Files

The parquet files required to run the API and Streamlit app are not 
included in this repository due to size constraints.

### How to generate them
1. Run the Databricks notebooks in `databricks/` in numbered order
2. Run the export script in `databricks/gold/` to export Gold tables
   to your Databricks Volume by modifying the volume path
3. Download from your Volume and place under `data/`
