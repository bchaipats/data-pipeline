from dataxml.common import Task


class MLTask(Task):
    def load_data(self):
        db = self.conf["input"].get("database", "default")
        table = self.conf["input"]["table"]
        self.logger.info(f"Reading dataset from {db}.{table}")
        _data = self.spark.table(f"{db}.{table}").toPandas()
        self.logger.info(f"Loaded dataset, total_size: {len(_data)}")
        return _data

    def launch(self):
        df = self.load_data()
        print(df.head())


def entrypoint():
    task = MLTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
