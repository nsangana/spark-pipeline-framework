"""Generate Lakeflow Declarative Pipeline SQL from YAML configs."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class LDPSQLGenerator:
    """Converts Spark Pipeline Framework YAML to LDP SQL."""

    def __init__(self, config_path: str):
        """Initialize generator with config path.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.pipeline_name = self.config["pipeline"]["name"]
        self.sources = self.config.get("sources", [])
        self.transformations = self.config.get("transformations", [])
        self.validation = self.config.get("validation", {})
        self.target = self.config.get("target", {})

    def generate_sql(self) -> str:
        """Generate complete LDP SQL code.

        Returns:
            SQL code string for Lakeflow Declarative Pipeline
        """
        sql_parts = []

        # Header comment
        sql_parts.append(self._generate_header())

        # Generate source tables (streaming or materialized views)
        for source in self.sources:
            sql_parts.append(self._generate_source_table(source))

        # Generate transformation tables
        for i, transform in enumerate(self.transformations):
            is_final = i == len(self.transformations) - 1
            sql_parts.append(self._generate_transformation_table(transform, is_final))

        return "\n\n".join(sql_parts)

    def _generate_header(self) -> str:
        """Generate SQL header comment."""
        return f"""-- Lakeflow Declarative Pipeline: {self.pipeline_name}
-- Generated from: {self.config_path.name}
-- Description: {self.config['pipeline'].get('description', 'N/A')}
--
-- This pipeline was auto-generated from Spark Pipeline Framework YAML configuration."""

    def _generate_source_table(self, source: Dict) -> str:
        """Generate SQL for source table.

        Args:
            source: Source configuration dict

        Returns:
            SQL CREATE statement
        """
        name = source["name"]
        source_type = source["type"]
        path = source["path"]

        # Special handling: dimension tables should be MATERIALIZED VIEW (batch)
        # Common dimension table names to treat as batch
        dimension_keywords = ["user_profiles", "users", "customers", "products",
                             "categories", "dimensions", "lookup", "reference"]
        is_dimension = any(keyword in name.lower() for keyword in dimension_keywords)

        # Determine if this is streaming - dimensions are always batch
        is_streaming = source_type in ["delta", "json", "csv", "parquet"] and not is_dimension

        if is_streaming:
            # Create streaming table for event/fact sources
            if source_type == "delta":
                sql = f"""CREATE OR REFRESH STREAMING TABLE {name}
AS SELECT * FROM STREAM delta.`{path}`;"""
            else:
                sql = f"""CREATE OR REFRESH STREAMING TABLE {name}
AS SELECT * FROM STREAM read_files(
  '{path}',
  format => '{source_type}'
);"""
            comment = f"-- Source: {name} (streaming)"
        else:
            # Materialized view for dimensions or batch sources
            if source_type == "delta":
                sql = f"""CREATE OR REFRESH MATERIALIZED VIEW {name}
AS SELECT * FROM delta.`{path}`;"""
            else:
                sql = f"""CREATE OR REFRESH MATERIALIZED VIEW {name}
AS SELECT * FROM read_files(
  '{path}',
  format => '{source_type}'
);"""
            comment = f"-- Source: {name} (materialized view - {'dimension table' if is_dimension else 'batch'})"

        return f"{comment}\n{sql}"

    def _generate_transformation_table(self, transform: Dict, is_final: bool) -> str:
        """Generate SQL for transformation table.

        Args:
            transform: Transformation configuration dict
            is_final: Whether this is the final transformation

        Returns:
            SQL CREATE statement
        """
        name = transform["output"]
        transform_type = transform["type"]

        # For final transformation, add validation expectations
        expectations = []
        if is_final and self.validation.get("enabled"):
            expectations = self._generate_expectations()

        # Determine table type based on inputs and whether it's final
        inputs = transform.get("inputs", [])
        has_streaming = self._has_streaming_inputs(inputs)
        has_batch = self._has_batch_inputs(inputs)

        # Rule 1: Final aggregations should be MATERIALIZED VIEW (for efficient querying)
        if is_final:
            is_streaming = False
            logger.info(f"Transformation '{name}': MATERIALIZED VIEW (final output)")

        # Rule 2: Stream-static join pattern (streaming + batch dimension)
        # If primary input is streaming and batch inputs are dimensions, keep it streaming
        elif has_streaming and has_batch:
            # Check if batch inputs are dimension tables
            dimension_keywords = ["user_profiles", "users", "customers", "products",
                                 "categories", "dimensions", "lookup", "reference"]
            batch_inputs = [inp for inp in inputs if self._is_batch_table(inp)]
            all_dimensions = all(any(kw in inp.lower() for kw in dimension_keywords)
                               for inp in batch_inputs)

            if all_dimensions:
                is_streaming = True
                logger.info(f"Transformation '{name}': STREAMING TABLE (stream-static join with dimensions)")
            else:
                is_streaming = False
                logger.info(f"Transformation '{name}': MATERIALIZED VIEW (mixed inputs, non-dimension batch)")

        # Rule 3: All inputs are streaming, keep it streaming
        elif has_streaming:
            is_streaming = True
            logger.info(f"Transformation '{name}': STREAMING TABLE (all streaming inputs)")

        # Rule 4: All inputs are batch, output is batch
        else:
            is_streaming = False
            logger.info(f"Transformation '{name}': MATERIALIZED VIEW (all batch inputs)")

        # Build the SQL with descriptive comments
        if is_streaming:
            table_type = "STREAMING TABLE"
            type_comment = "streaming transformation"
        else:
            table_type = "MATERIALIZED VIEW"
            if is_final:
                type_comment = "final aggregation"
            else:
                type_comment = "batch transformation"

        # Add expectations clause if any
        expectations_clause = ""
        if expectations:
            expectations_clause = "(\n  " + ",\n  ".join(expectations) + "\n)"

        # Get the transformation SQL
        if transform_type == "sql":
            transform_sql = transform["sql"].strip()

            # Fix streaming incompatibilities
            transform_sql = self._fix_streaming_sql(transform_sql, is_streaming)

            # If this is a streaming table, wrap streaming inputs with STREAM()
            if is_streaming:
                transform_sql = self._wrap_streaming_sources(transform_sql, inputs)
        else:
            # For non-SQL transformations, we'd need to convert logic
            # For now, create a placeholder
            logger.warning(
                f"Non-SQL transformation '{transform_type}' detected. "
                f"Manual conversion required for LDP."
            )
            transform_sql = f"-- TODO: Convert {transform_type} transformation to SQL"

        sql = f"""CREATE OR REFRESH {table_type} {name}{expectations_clause}
AS {transform_sql};"""

        return f"-- Transformation: {name} ({type_comment})\n{sql}"

    def _fix_streaming_sql(self, sql: str, is_streaming: bool) -> str:
        """Fix SQL for streaming compatibility.

        Args:
            sql: Original SQL query
            is_streaming: Whether this is for a streaming table

        Returns:
            Fixed SQL query
        """
        import re

        if not is_streaming:
            return sql

        # Fix 1: Replace COUNT(DISTINCT x) with approx_count_distinct(x)
        # Pattern: COUNT(DISTINCT column_name)
        sql = re.sub(
            r'COUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)',
            r'approx_count_distinct(\1)',
            sql,
            flags=re.IGNORECASE
        )

        # Fix 2: Add NULL check for aggregation keys if not present
        # This helps with watermark requirements (though full watermark support needs more work)

        return sql

    def _wrap_streaming_sources(self, sql: str, inputs: List[str]) -> str:
        """Wrap streaming source tables with STREAM() function.

        Args:
            sql: SQL query string
            inputs: List of input table names

        Returns:
            Modified SQL with STREAM() wrappers
        """
        import re

        # Identify streaming vs batch sources
        streaming_sources = []
        batch_sources = []

        # Dimension table keywords
        dimension_keywords = ["user_profiles", "users", "customers", "products",
                             "categories", "dimensions", "lookup", "reference"]

        for source in self.sources:
            if source["name"] in inputs:
                is_dimension = any(kw in source["name"].lower() for kw in dimension_keywords)
                if is_dimension:
                    batch_sources.append(source["name"])
                elif source["type"] in ["delta", "json", "csv", "parquet"]:
                    streaming_sources.append(source["name"])

        # Check transformations that are inputs
        # Use the same logic as table type determination
        for i, transform in enumerate(self.transformations):
            if transform["output"] in inputs:
                transform_inputs = transform.get("inputs", [])
                is_final = (i == len(self.transformations) - 1)

                # Apply same rules as _generate_transformation_table
                has_streaming = self._has_streaming_inputs(transform_inputs)
                has_batch = self._has_batch_inputs(transform_inputs)

                # Determine if this transformation is streaming
                if is_final:
                    # Final is always batch (materialized view)
                    batch_sources.append(transform["output"])
                elif has_streaming and has_batch:
                    # Stream-static join - still streaming
                    batch_inputs = [inp for inp in transform_inputs if self._is_batch_table(inp)]
                    all_dimensions = all(any(kw in inp.lower() for kw in dimension_keywords)
                                       for inp in batch_inputs)
                    if all_dimensions:
                        streaming_sources.append(transform["output"])
                    else:
                        batch_sources.append(transform["output"])
                elif has_streaming:
                    streaming_sources.append(transform["output"])
                else:
                    batch_sources.append(transform["output"])

        # Wrap only streaming sources with STREAM()
        # Batch sources (dimensions) should NOT be wrapped
        for source_name in streaming_sources:
            patterns = [
                (rf'\bFROM\s+{source_name}\b', f'FROM STREAM({source_name})'),
                (rf'\bJOIN\s+{source_name}\b', f'JOIN STREAM({source_name})'),
            ]

            for pattern, replacement in patterns:
                sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        return sql

    def _generate_expectations(self) -> List[str]:
        """Generate data quality expectations from validation rules.

        Returns:
            List of SQL CONSTRAINT clauses
        """
        expectations = []
        rules = self.validation.get("rules", [])

        for rule in rules:
            rule_name = rule["name"]
            rule_type = rule["type"]

            if rule_type == "null_check":
                column = rule["column"]
                expectations.append(
                    f"CONSTRAINT {rule_name} EXPECT ({column} IS NOT NULL) ON VIOLATION DROP ROW"
                )

            elif rule_type == "range_check":
                column = rule["column"]
                conditions = []
                if "min_value" in rule:
                    conditions.append(f"{column} >= {rule['min_value']}")
                if "max_value" in rule:
                    conditions.append(f"{column} <= {rule['max_value']}")

                if conditions:
                    condition_sql = " AND ".join(conditions)
                    expectations.append(
                        f"CONSTRAINT {rule_name} EXPECT ({condition_sql}) ON VIOLATION DROP ROW"
                    )

            elif rule_type == "row_count":
                # Row count checks can't be expressed as row-level expectations
                # These would need to be monitored separately
                logger.warning(f"Row count rule '{rule_name}' cannot be converted to LDP expectation")

        return expectations

    def _has_streaming_inputs(self, inputs: List[str]) -> bool:
        """Check if any inputs are streaming tables.

        Args:
            inputs: List of input table names

        Returns:
            True if any input is a streaming source
        """
        dimension_keywords = ["user_profiles", "users", "customers", "products",
                             "categories", "dimensions", "lookup", "reference"]

        # Check sources
        for source in self.sources:
            if source["name"] in inputs:
                is_dimension = any(kw in source["name"].lower() for kw in dimension_keywords)
                if not is_dimension and source["type"] in ["delta", "json", "csv", "parquet"]:
                    return True

        # Check transformations (recursively)
        for transform in self.transformations:
            if transform["output"] in inputs:
                transform_inputs = transform.get("inputs", [])
                if self._has_streaming_inputs(transform_inputs):
                    return True

        return False

    def _is_batch_table(self, table_name: str) -> bool:
        """Check if a table is a batch (materialized view) table.

        Args:
            table_name: Table name to check

        Returns:
            True if table is batch
        """
        dimension_keywords = ["user_profiles", "users", "customers", "products",
                             "categories", "dimensions", "lookup", "reference"]

        # Check sources
        for source in self.sources:
            if source["name"] == table_name:
                is_dimension = any(kw in source["name"].lower() for kw in dimension_keywords)
                return is_dimension

        # Check transformations (recursively)
        for transform in self.transformations:
            if transform["output"] == table_name:
                transform_inputs = transform.get("inputs", [])
                return self._has_batch_inputs(transform_inputs)

        return False

    def _has_batch_inputs(self, inputs: List[str]) -> bool:
        """Check if any inputs are batch (materialized view) tables.

        Args:
            inputs: List of input table names

        Returns:
            True if any input is a batch source
        """
        dimension_keywords = ["user_profiles", "users", "customers", "products",
                             "categories", "dimensions", "lookup", "reference"]

        # Check sources
        for source in self.sources:
            if source["name"] in inputs:
                is_dimension = any(kw in source["name"].lower() for kw in dimension_keywords)
                if is_dimension:
                    return True

        # Check transformations (recursively)
        for i, transform in enumerate(self.transformations):
            if transform["output"] in inputs:
                # Check if this transformation itself is batch (comes before current one)
                transform_inputs = transform.get("inputs", [])
                if self._has_batch_inputs(transform_inputs):
                    return True

        return False

    def save_sql(self, output_path: str) -> None:
        """Generate and save SQL to file.

        Args:
            output_path: Path to save generated SQL file
        """
        sql = self.generate_sql()
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            f.write(sql)

        logger.info(f"Generated LDP SQL saved to: {output_file}")
        print(f"✓ Generated LDP SQL: {output_file}")
