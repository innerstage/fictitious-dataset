import pandas as pd
import sys
import csv
import os

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter, LoopHelper
from bamboo_lib.steps import LoadStep


class IterStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Generating chunk of data...")

        filename = "source_data/sales_{}.csv".format(params.get("year"))
        df_gen = pd.read_csv(filename, keep_default_na=False, chunksize=100000)
        
        return df_gen


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Transforming chunk of data...")

        df = prev

        # Date Dimension
        df["date_id"] = df["date"].str.replace("-","").astype(int)
        df = df.drop(columns="date")

        # Origin Country Dimension
        dim_df = pd.read_csv("data_temp/dim_origin_country.csv")
        dim_dict = {k:v for (k,v) in zip(dim_df["origin_country_iso2"], dim_df["origin_country_id"])}

        df["origin_country_id"] = df["origin_country"].map(dim_dict).fillna(0)
        df = df.drop(columns="origin_country")

        # Destination Country Dimension
        dim_df = pd.read_csv("data_temp/dim_destination_country.csv")
        dim_dict = {k:v for (k,v) in zip(dim_df["destination_country_iso2"], dim_df["destination_country_id"])}

        df["destination_country_id"] = df["destination_country"].map(dim_dict).fillna(0)
        df = df.drop(columns="destination_country")

        # Product Dimension
        prod = pd.read_csv("source_data/product_list.csv")
        df = df.merge(prod, how="left", left_on="product", right_on="product_name")

        dim_df = pd.read_csv("data_temp/dim_product.csv")
        dim_dict = {k:v for (k,v) in zip(dim_df["product_name"], dim_df["product_id"])}
        df["product_id"] = df["product_name"].map(dim_dict).fillna(0)
        df = df.drop(columns="product")

        # Category Dimension
        df["category_id"] = df["product_category"].map({"fruit": 1, "vegetable": 2}).fillna(0)
        df = df.drop(columns="product_category")

        # Color Dimension
        dim_df = pd.read_csv("data_temp/dim_color.csv")
        dim_dict = {k:v for (k,v) in zip(dim_df["color_name"].str.lower(), dim_df["color_id"])}
        df["color_id"] = df["product_color"].map(dim_dict).fillna(0)
        df = df.drop(columns="product_color")

        # Tidying Up
        df = df[[
            "date_id", "origin_country_id", "destination_country_id", "product_id", "category_id", 
            "color_id", "duration", "units", "amount"
        ]]

        df.to_csv("data_temp/sales_fact.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, mode="a")

        return df


class SalesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool),
            Parameter("year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("output-db"))

        iter_step = IterStep()
        transform_step = TransformStep()

        load_step = LoadStep(
            table_name="sales_fact", 
            connector=db_connector, 
            if_exists="append", 
            pk=["date_id"],
            dtype={
                "date_id": "Int64",
                "origin_country_id": "Int64", 
                "destination_country_id": "Int64", 
                "product_id": "Int64", 
                "category_id": "Int64", 
                "color_id": "Int64", 
                "duration": "Float64", 
                "units": "Int64", 
                "amount": "Int64"
            },
            nullable_list=[]
        )

        if params.get("ingest")==True:
            steps = [LoopHelper(iter_step=iter_step, sub_steps=[transform_step, load_step])]
        else:
            steps = [LoopHelper(iter_step=iter_step, sub_steps=[transform_step])]

        return steps


if __name__ == "__main__":
    sales_pipeline = SalesPipeline()
    sales_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True,
            "year": str(sys.argv[1])
        }
    )