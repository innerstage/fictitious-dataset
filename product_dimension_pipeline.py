import pandas as pd
import sys
import csv
import os

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class ProductStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Product Dimension Table...")

        df = pd.read_csv("source_data/product_list.csv")
        df["product_id"] = df.index + 1
        df = df[["product_id", "product_name"]]

        zero_df = pd.DataFrame({
            "product_id": [0],
            "product_name": ["Undefined"]
        })

        df = pd.concat([df, zero_df], ignore_index=True)

        df = df.sort_values(by="product_id")

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_product.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class CategoryStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Category Dimension Table...")

        df = pd.DataFrame({
            "category_id": [1,2],
            "category_name": ["Fruit", "Vegetable"]
        })

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_category.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class ColorStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Color Dimension Table...")

        df = pd.read_csv("source_data/product_list.csv")
        df = df[["product_color"]].drop_duplicates()
        df["product_color"] = df["product_color"].str.title()
        df = df.reset_index(drop=True)
        df["color_id"] = df.index + 1
        df = df.rename(columns={"product_color": "color_name"})

        df = df[["color_id", "color_name"]]

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_color.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class ProductPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("output-db"))

        product_step = ProductStep()
        category_step = CategoryStep()
        color_step = ColorStep()

        load_product = LoadStep(
            table_name="dim_product", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["product_id"],
            dtype={
                "product_id": "Int64",
                "product_name": "String"
            },
            nullable_list=[]
        )

        load_category = LoadStep(
            table_name="dim_category", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["category_id"],
            dtype={
                "category_id": "Int64",
                "category_name": "String"
            },
            nullable_list=[]
        )

        load_color = LoadStep(
            table_name="dim_color", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["color_id"],
            dtype={
                "color_id": "Int64",
                "color_name": "String"
            },
            nullable_list=[]
        )

        if params.get("ingest")==True:
            steps = [product_step, load_product, category_step, load_category, color_step, load_color]
        else:
            steps = [product_step, category_step, color_step]

        return steps


if __name__ == "__main__":
    product_pipeline = ProductPipeline()
    product_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )