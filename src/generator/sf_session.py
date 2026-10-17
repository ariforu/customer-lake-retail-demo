"""Holds the Databricks Connect session outside the `gen` package (which is pickled by value into UDFs)."""
_SPARK = None


def get(profile, register_pkg):
    global _SPARK
    if _SPARK is None:
        from databricks.connect import DatabricksEnv, DatabricksSession
        from pyspark import cloudpickle
        cloudpickle.register_pickle_by_value(register_pkg)
        env = DatabricksEnv().withDependencies("faker")
        _SPARK = DatabricksSession.builder.profile(profile).withEnvironment(env).serverless(True).getOrCreate()
    return _SPARK


def get_runtime(register_pkg):
    """Inside a Databricks job: reuse the active session, still ship `gen` by value to Python workers."""
    global _SPARK
    if _SPARK is None:
        from pyspark import cloudpickle
        from pyspark.sql import SparkSession
        cloudpickle.register_pickle_by_value(register_pkg)
        _SPARK = SparkSession.builder.getOrCreate()
    return _SPARK
