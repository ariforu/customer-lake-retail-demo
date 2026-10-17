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
    """Inside a Databricks job/notebook: reuse the active session and ship the package to Python workers as a
    zipped artifact (pickling notebook-imported modules by value would drag in the workspace import hook)."""
    global _SPARK
    if _SPARK is None:
        import os
        import tempfile
        import zipfile
        from pyspark.sql import SparkSession
        _SPARK = SparkSession.builder.getOrCreate()
        pkg_dir = os.path.dirname(os.path.abspath(register_pkg.__file__))
        zpath = os.path.join(tempfile.mkdtemp(), f"{register_pkg.__name__}.zip")
        with zipfile.ZipFile(zpath, "w") as z:
            for root, _, files in os.walk(pkg_dir):
                for f in files:
                    if f.endswith(".py"):
                        full = os.path.join(root, f)
                        z.write(full, os.path.relpath(full, os.path.dirname(pkg_dir)))
        _SPARK.addArtifacts(zpath, pyfile=True)
    return _SPARK
