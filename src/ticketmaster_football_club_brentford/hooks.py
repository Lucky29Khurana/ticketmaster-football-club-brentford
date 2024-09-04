from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
import os
import configparser
from typing import Any, Dict

logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


class AthenaSparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        logging.info(
            f" Load the Athena spark configuration in spark.yml using the config "
            f"loader."
        )
        # Load spark configuration settings from spark.yml
        parameters = context.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        # Load the aws authentication tokens
        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.aws/credentials"))

        # here 'default' is the name of the aws profile, this may need to be adapted
        access_key = config.get("default", "aws_access_key_id")
        secret_key = config.get("default", "aws_secret_access_key")
        session_key = config.get("default", "aws_session_token")

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
            # load the appropriate jar files (1 and 2 for s3 connection and 3 is for
            # the AWS Athena Driver - they must be in this order to work)
            .config(
                "spark.jars",
                f"{context.project_path}/s3_jar_files/hadoop-aws-3.3.4.jar,"
                f"{context.project_path}/s3_jar_files/aws-java-sdk-bundle-1.12.262.jar,"
                f"{context.project_path}/athena_jar_files/AthenaJDBC42-2.1.1.1000.jar"
            )
        )
        _spark_session = spark_session_conf.getOrCreate()

        # save the AWS authentication tokens to the spark session to enable writing
        # to the s3 database.

        _spark_session._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )
        _spark_session._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        _spark_session._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        _spark_session._jsc.hadoopConfiguration().set(
            "fs.s3a.session.token", session_key)

        # set the log level (DEBUG for debugging)
        _spark_session.sparkContext.setLogLevel("WARN")


class PySparkHooks:
    @hook_impl
    def after_context_created(self, context):
        self.context = context
    @hook_impl
    def before_pipeline_run(
            self, run_params: Dict[str, Any], pipeline) -> None:

        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        # Logic to only run Hook for specific pipelines.

        for node in pipeline.nodes:

            if node.name in (
                [
                    # insert node name
                ]
            ):
                spark_required = True
                break
            else:
                spark_required = False

            # only create spark session if spark_required is true

        if spark_required:

            config_loader = self.context.config_loader
            # explicitly define the project path for use on databricks
            project_path = self.context.project_path
            # update the config parameters here, as when it is updated in setting.py
            # it resets the default run_env to base.

            config_loader.config_patterns = {
                'catalog': ['catalog*', 'catalog*/**', '**/catalog*'],
                'parameters': ['parameters*', 'parameters*/**', '**/parameters*'],
                'credentials': ['credentials*', 'credentials*/**', '**/credentials*'],
                'globals': ['globals.yml'], "spark": ["spark*", "spark*/**"]}

            logging.info(
                f" Spark Required! Load the Athena spark configuration in spark.yml "
                f"using the config loader."
            )
            # Load spark configuration settings from spark.yml
            parameters = config_loader.get("spark")
            spark_conf = SparkConf().setAll(parameters.items())

            # different options for local and databricks project run through

            if 'Workspace' in str(project_path):

                # This is the databricks option
                logger.info('Databricks Spark Session Attempt')

                # Initialise the spark session
                spark_session_conf = (
                    SparkSession.builder.appName(self.context.project_path.name)
                    .enableHiveSupport()
                    .config(conf=spark_conf)
                    # load the appropriate jar files (1 and 2 for s3 connection and 3 is for
                    # the AWS Athena Driver - they must be in this order to work)
                    .config(
                        "spark.jars",
                        "dbfs:/FileStore/jars/1323cbd6_a4b4_44b6_a09c_5a17e481f701-hadoop_aws_3_3_4-d33a1.jar,"
                        "dbfs:/FileStore/jars/af0c6e8c_f067_4b87_8cd9_a10303916d88-aws_java_sdk_bundle_1_12_262-04b39.jar,"
                        "dbfs:/FileStore/jars/eef11388_6a40_4c51_890a_23bcd52dd82b-AthenaJDBC42_2_1_1_1000-bb28b.jar"
                    )
                )
                _spark_session = spark_session_conf.getOrCreate()

                # set the log level (DEBUG for debugging)
                _spark_session.sparkContext.setLogLevel("WARN")


            else:

                config = configparser.ConfigParser()
                config.read(os.path.expanduser("~/.aws/credentials"))

                # here 'default' is the name of the aws profile,
                # this may need to be adapted

                # if you are changing the aws profile name you should also ensure
                # that the catalog entry for the data you are tyring to load contains
                # the new aws profile name and not default

                access_key = config.get("default", "aws_access_key_id")
                secret_key = config.get("default", "aws_secret_access_key")
                session_key = config.get("default", "aws_session_token")

                logger.info(f"Project Path is: {self.context.project_path}")

                # Initialise the spark session
                spark_session_conf = (
                    SparkSession.builder.appName(self.context.project_path.name)
                    .enableHiveSupport()
                    .config(conf=spark_conf)
                    # load the appropriate jar files (1 and 2 for s3 connection and 3 is for
                    # the AWS Athena Driver - they must be in this order to work)
                    .config(
                        "spark.jars",
                        f"{self.context.project_path}/s3_jar_files/hadoop-aws-3.3.4"
                        f".jar,"
                        f"{self.context.project_path}/s3_jar_files/aws-java-sdk"
                        f"-bundle-1.12.262.jar,"
                        f"{self.context.project_path}/athena_jar_files/AthenaJDBC42-2"
                        f".1.1.1000.jar"
                    )
                )

                _spark_session = spark_session_conf.getOrCreate()

                # save the AWS authentication tokens to the spark session to enable writing
                # to the s3 database.

                _spark_session._jsc.hadoopConfiguration().set(
                    "fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
                )
                _spark_session._jsc.hadoopConfiguration().set(
                    "fs.s3a.access.key", access_key)
                _spark_session._jsc.hadoopConfiguration().set(
                    "fs.s3a.secret.key", secret_key)
                _spark_session._jsc.hadoopConfiguration().set(
                    "fs.s3a.session.token", session_key)

                # set the log level (DEBUG for debugging)
                _spark_session.sparkContext.setLogLevel("WARN")

        else:
            logger.info(
                'Spark not required for this pipeline. No spark session initiated.'
            )
