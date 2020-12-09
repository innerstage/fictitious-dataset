import pandas as pd
import sys
import csv
import os

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class CreateStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Date Dimension Table...")

        dates = pd.date_range(start=params.get("first-month"), end=params.get("last-month"), freq="D")

        df = pd.DataFrame({"date": dates})

        df["date_id"] = (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2) + df.date.dt.day.astype(str).str.zfill(2)).astype(int) 
        df["year"] = df.date.dt.year.astype(int)
        df["quarter"] = df.date.dt.year.astype(str) + "-Q" + df.date.dt.quarter.astype(str)
        df["month"] = (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2)).astype(int)
        df["month_name"] = df.date.dt.strftime("%B, %Y")
        df["day_name"] = df.date.dt.strftime("%B %d, %Y")

        df = df[["date_id", "year", "quarter", "month", "month_name", "day_name"]]

        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")

        df.to_csv("data_temp/dim_date.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class DatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool),
            Parameter("first-month", dtype=str),
            Parameter("last-month", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("output-db"))

        create_step = CreateStep()

        load_step = LoadStep(
            table_name="dim_date", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["date_id"],
            dtype={
                "date_id": "Int64",
                "year": "Int64",
                "quarter": "String",
                "month": "Int64",
                "month_name": "String",
                "day_name": "String"
            },
            nullable_list=[]
        )

        if params.get("ingest")==True:
            steps = [create_step, load_step]
        else:
            steps = [create_step]

        return steps


if __name__ == "__main__":
    date_pipeline = DatePipeline()
    date_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True,
            "first-month": sys.argv[1],
            "last-month": sys.argv[2]
        }
    )