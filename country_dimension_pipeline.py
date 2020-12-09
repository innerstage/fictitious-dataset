import pandas as pd
import sys
import csv
import os

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class OriginStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Origin Country Dimension Table...")

        df = pd.read_csv("source_data/country_list.csv", keep_default_na=False)

        df = df.rename(columns={"country_name": "origin_country_name", "iso2_code": "origin_country_iso2"})
        df["origin_country_id"] = df.index + 1

        zero_df = pd.DataFrame({
            "origin_country_id": [0],
            "origin_country_name": ["Undefined"],
            "origin_country_iso2": ["XX"]
        })

        df = pd.concat([df, zero_df], ignore_index=True, sort=False)

        df = df[["origin_country_id", "origin_country_name", "origin_country_iso2"]]
        df = df.sort_values(by="origin_country_id")
        #print(df[df.origin_country_iso2.isnull()])

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_origin_country.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class DestinationStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Destination Country Dimension Table...")

        df = pd.read_csv("source_data/country_list.csv", keep_default_na=False)

        df = df.rename(columns={"country_name": "destination_country_name", "iso2_code": "destination_country_iso2"})
        df["destination_country_id"] = df.index + 1

        zero_df = pd.DataFrame({
            "destination_country_id": [0],
            "destination_country_name": ["Undefined"],
            "destination_country_iso2": ["XX"]
        })

        df = pd.concat([df, zero_df], ignore_index=True, sort=False)

        df = df[["destination_country_id", "destination_country_name", "destination_country_iso2"]]
        df = df.sort_values(by="destination_country_id")

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_destination_country.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class CountryPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("output-db"))

        origin_step = OriginStep()
        destination_step = DestinationStep()

        load_origin = LoadStep(
            table_name="dim_origin_country", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["origin_country_id"],
            dtype={
                "origin_country_id": "Int64",
                "origin_country_name": "String",
                "origin_country_iso2": "String"
            },
            nullable_list=[]
        )

        load_destination = LoadStep(
            table_name="dim_destination_country", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["destination_country_id"],
            dtype={
                "destination_country_id": "Int64",
                "destination_country_name": "String",
                "destination_country_iso2": "String"
            },
            nullable_list=[]
        )

        if params.get("ingest")==True:
            steps = [origin_step, load_origin, destination_step, load_destination]
        else:
            steps = [origin_step, destination_step]

        return steps


if __name__ == "__main__":
    country_pipeline = CountryPipeline()
    country_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )