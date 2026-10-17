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
    """Inside a Databricks job/notebook: reuse the active session and make the package importable on Python workers.

    Serverless / Spark Connect: upload a zip as a pyfile artifact. Classic clusters: SparkContext.addPyFile.
    The zip name carries a content hash, so re-running after a Python restart is a no-op for unchanged code and a
    fresh, non-conflicting upload for changed code. It is written to a fixed temp path (no per-run temp dirs)."""
    global _SPARK
    if _SPARK is None:
        import hashlib
        import os
        import tempfile
        import zipfile
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        pkg_dir = os.path.dirname(os.path.abspath(register_pkg.__file__))
        files = sorted(os.path.join(r, f) for r, _, fs in os.walk(pkg_dir) for f in fs if f.endswith(".py"))
        digest = hashlib.sha256(b"".join(open(f, "rb").read() for f in files)).hexdigest()[:12]
        zpath = os.path.join(tempfile.gettempdir(), f"{register_pkg.__name__}_{digest}.zip")
        if not os.path.exists(zpath):
            with zipfile.ZipFile(zpath, "w") as z:
                for f in files:
                    z.write(f, os.path.relpath(f, os.path.dirname(pkg_dir)))
        if type(spark).__module__.startswith("pyspark.sql.connect"):
            try:
                spark.addArtifacts(zpath, pyfile=True)
            except Exception as e:  # same content already uploaded to this session
                if "already exists" not in str(e).lower():
                    raise
        else:
            spark.sparkContext.addPyFile(zpath)
        _SPARK = spark
    return _SPARK
