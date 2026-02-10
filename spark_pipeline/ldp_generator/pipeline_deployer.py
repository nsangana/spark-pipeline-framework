"""Deploy Lakeflow Declarative Pipelines to Databricks workspace."""

import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .config import LDPConfig, load_config

logger = logging.getLogger(__name__)


class LDPDeployer:
    """Deploy Lakeflow Declarative Pipelines using Databricks API."""

    def __init__(
        self,
        config: Optional[LDPConfig] = None,
        profile: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """Initialize deployer.

        Args:
            config: LDPConfig instance (or will be loaded from env/file)
            profile: Databricks CLI profile to use
            catalog: Unity Catalog name
            schema: Schema name
        """
        if config:
            self.config = config
        else:
            self.config = load_config(
                profile=profile,
                catalog=catalog,
                schema=schema,
            )

        self.profile = self.config.profile
        self.catalog = self.config.catalog
        self.schema = self.config.schema

    def create_pipeline(
        self,
        name: str,
        sql_file_path: str,
        catalog: str,
        schema: str,
        storage_location: Optional[str] = None,
        description: Optional[str] = None,
        continuous: bool = False,
        development: bool = True,
    ) -> Dict:
        """Create a Lakeflow Declarative Pipeline in Databricks.

        Args:
            name: Pipeline name
            sql_file_path: Path to SQL file containing pipeline code
            catalog: Unity Catalog name
            schema: Schema name
            storage_location: Storage location for pipeline tables
            description: Pipeline description
            continuous: Run continuously (True) or triggered (False)
            development: Development mode (True) or production (False)

        Returns:
            Pipeline creation result with pipeline_id
        """
        logger.info(f"Creating LDP pipeline: {name}")

        # Upload SQL file to workspace
        workspace_path = self._upload_sql_to_workspace(sql_file_path, name)

        # Build pipeline configuration JSON
        pipeline_config = self._build_pipeline_config(
            name=name,
            workspace_path=workspace_path,
            catalog=catalog,
            schema=schema,
            storage_location=storage_location,
            description=description,
            continuous=continuous,
            development=development,
        )

        # Create pipeline using Databricks CLI
        result = self._create_pipeline_via_cli(pipeline_config)

        logger.info(f"Pipeline created successfully: {result.get('pipeline_id')}")
        return result

    def _upload_sql_to_workspace(self, sql_file_path: str, pipeline_name: str) -> str:
        """Upload SQL file to Databricks workspace.

        Args:
            sql_file_path: Local path to SQL file
            pipeline_name: Pipeline name for organizing files

        Returns:
            Workspace path where file was uploaded
        """
        sql_file = Path(sql_file_path)
        workspace_dir = self.config.get_workspace_base_path()
        workspace_path = f"{workspace_dir}/{pipeline_name}.sql"

        # Create directory if it doesn't exist
        mkdir_cmd = [
            "databricks",
            "workspace",
            "mkdirs",
            workspace_dir,
            "--profile",
            self.profile,
        ]
        try:
            subprocess.run(mkdir_cmd, capture_output=True, text=True, check=False)
            logger.info(f"Ensured directory exists: {workspace_dir}")
        except Exception as e:
            logger.warning(f"Could not create directory (may already exist): {e}")

        logger.info(f"Uploading {sql_file} to {workspace_path}")

        cmd = [
            "databricks",
            "workspace",
            "import",
            workspace_path,
            "--file",
            str(sql_file),
            "--language",
            "SQL",
            "--overwrite",
            "--profile",
            self.profile,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"File uploaded successfully to {workspace_path}")
            return workspace_path
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upload file: {e.stderr}")
            raise RuntimeError(f"Failed to upload SQL file: {e.stderr}")

    def _build_pipeline_config(
        self,
        name: str,
        workspace_path: str,
        catalog: str,
        schema: str,
        storage_location: Optional[str],
        description: Optional[str],
        continuous: bool,
        development: bool,
    ) -> Dict:
        """Build pipeline configuration JSON.

        Args:
            name: Pipeline name
            workspace_path: Path to SQL file in workspace
            catalog: Unity Catalog name
            schema: Schema name
            storage_location: Storage location
            description: Description
            continuous: Continuous mode
            development: Development mode

        Returns:
            Pipeline configuration dict
        """
        config = {
            "name": name,
            "catalog": catalog,
            "target": schema,
            "libraries": [{"notebook": {"path": workspace_path}}],
            "continuous": continuous,
            "development": development,
        }

        # Note: description field is not supported in pipeline API
        # Storage location is not needed when using Unity Catalog
        # The catalog/target combination automatically handles storage

        # Use configuration settings
        config["serverless"] = self.config.serverless
        config["channel"] = self.config.channel
        config["edition"] = self.config.edition

        return config

    def _create_pipeline_via_cli(self, pipeline_config: Dict) -> Dict:
        """Create pipeline using Databricks CLI.

        Args:
            pipeline_config: Pipeline configuration dict

        Returns:
            Pipeline creation result
        """
        # Save config to temporary file
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(pipeline_config, f, indent=2)
            config_file = f.name

        try:
            cmd = [
                "databricks",
                "pipelines",
                "create",
                "--json",
                f"@{config_file}",
                "--profile",
                self.profile,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            result_data = json.loads(result.stdout)

            logger.info(f"Pipeline created: {result_data}")
            return result_data

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create pipeline: {e.stderr}")
            raise RuntimeError(f"Failed to create pipeline: {e.stderr}")

        finally:
            # Clean up temp file
            Path(config_file).unlink(missing_ok=True)

    def create_job_for_pipeline(
        self,
        job_name: str,
        pipeline_id: str,
        schedule_cron: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict:
        """Create a Databricks Job to orchestrate the pipeline.

        Args:
            job_name: Job name
            pipeline_id: Pipeline ID to run
            schedule_cron: Cron expression for scheduling (optional)
            description: Job description

        Returns:
            Job creation result with job_id
        """
        logger.info(f"Creating job for pipeline: {pipeline_id}")

        job_config = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": "run_pipeline",
                    "pipeline_task": {"pipeline_id": pipeline_id},
                }
            ],
        }

        if description:
            job_config["description"] = description

        if schedule_cron:
            job_config["schedule"] = {
                "quartz_cron_expression": schedule_cron,
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED",
            }

        # Save config to temporary file
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(job_config, f, indent=2)
            config_file = f.name

        try:
            cmd = [
                "databricks",
                "jobs",
                "create",
                "--json",
                f"@{config_file}",
                "--profile",
                self.profile,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            result_data = json.loads(result.stdout)

            logger.info(f"Job created: {result_data}")
            return result_data

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create job: {e.stderr}")
            raise RuntimeError(f"Failed to create job: {e.stderr}")

        finally:
            # Clean up temp file
            Path(config_file).unlink(missing_ok=True)

    def run_pipeline(self, pipeline_id: str, full_refresh: bool = False) -> Dict:
        """Trigger a pipeline run.

        Args:
            pipeline_id: Pipeline ID to run
            full_refresh: Perform full refresh (True) or incremental (False)

        Returns:
            Run result with update_id
        """
        logger.info(f"Starting pipeline run: {pipeline_id}")

        cmd = [
            "databricks",
            "pipelines",
            "start-update",
            pipeline_id,
            "--profile",
            self.profile,
        ]

        if full_refresh:
            cmd.append("--full-refresh")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            result_data = json.loads(result.stdout)

            logger.info(f"Pipeline run started: {result_data}")
            return result_data

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start pipeline: {e.stderr}")
            raise RuntimeError(f"Failed to start pipeline: {e.stderr}")

    def get_pipeline_status(self, pipeline_id: str) -> Dict:
        """Get pipeline status.

        Args:
            pipeline_id: Pipeline ID

        Returns:
            Pipeline status information
        """
        cmd = [
            "databricks",
            "pipelines",
            "get",
            pipeline_id,
            "--profile",
            self.profile,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return json.loads(result.stdout)

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get pipeline status: {e.stderr}")
            raise RuntimeError(f"Failed to get pipeline status: {e.stderr}")
