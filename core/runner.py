import pandas as pd
import json
from core.config_schema import SingleReportConfig, BatchReportConfig
from core.session_manager import SessionManager
from core.validator import PreExecutionValidator
from core.logger import get_logger

logger = get_logger(__name__)

class ReportRunner:

    def __init__(self, report_registry):
        self.report_registry = report_registry
        self.session_manager = SessionManager()

    def execute(self, config):

        if config.execution_mode == "single":
            self._run_single(config)

        elif config.execution_mode == "batch":
            self._run_batch(config)

    # --------------------------------
    # SINGLE EXECUTION
    # --------------------------------
    def _run_single(self, config):
        report_class = self.report_registry.get(config.report_name)

        if not report_class:
            raise ValueError(f"Unknown report: {config.report_name}")

        self.session = self.session_manager.get_session()
        self.session = self.session_manager.use_context(
            warehouse="TRICON_MEDIUM",
            database="SNOWFLAKE_STAGING",
            schema="TRICON"
        )

        # Validate
        validator = PreExecutionValidator(self.session)
        validator.validate(config)

        # Instantiate report
        report = report_class(self.session, config)
        # Execute pipeline
        queries = report.build_queries()

        raw_results = {}
        for name, sql in queries.items():
            logger.info(f"Executing query '{name}': {sql}")
            raw_results[name] = self.session.sql(sql).to_pandas()
            logger.info(f"Query '{name}' executed successfully, result shape: {raw_results[name].shape}")

        processed = report.process(raw_results)
        logger.info(f"Data processed successfully, processed data shape: {processed.shape}")

        output_path = report.generate_output(processed)

        logger.info(f"Report generated: {output_path}")
        return output_path

    # --------------------------------
    # BATCH EXECUTION
    # --------------------------------
    def _run_batch(self, batch_config: BatchReportConfig):

        records = []

        if batch_config.excel_path:
            df = pd.read_excel(batch_config.excel_path)
            records = df.to_dict(orient="records")

        elif batch_config.json_path:
            with open(batch_config.json_path) as f:
                records = json.load(f)

        elif batch_config.json_payload:
            records = batch_config.json_payload

        for row in records:
            single_config = SingleReportConfig(**row)
            self._run_single(single_config)
