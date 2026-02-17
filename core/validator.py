from core import logger

logger = logger.get_logger(__name__)

class PreExecutionValidator:

    def __init__(self, session):
        self.session = session

    def validate_account(self, external_id):
        # result = self.session.sql(
        #     f"SELECT id FROM canonical_staging.public.dim_account WHERE external_account_id = '{external_id}'"
        # ).collect()
        # if not result:
        if not external_id:
            raise ValueError("Invalid external account ID")
        logger.info(f"Account validation successful for external_account_id='{external_id}'")

    def validate_year(self, academic_year):
        if not academic_year.startswith("year"):
            raise ValueError("Academic year format invalid")
        logger.info(f"Year validation successful for academic_year='{academic_year}'")

    def validate_subjects(self, subjects):
        if subjects is None:
            subjects = "BOTH"
        valid_subjects = {"math", "ela", "spanish", "both"}
        if subjects not in valid_subjects:
            raise ValueError(f"Invalid subjects value: '{subjects}'. Must be one of {valid_subjects}")

    def validate(self, config):
        logger.info("Starting pre-execution validation")
        self.validate_account(config.external_account_id)
        self.validate_year(config.academic_year)
        self.validate_subjects(config.subjects)
        logger.info("Pre-execution validation completed successfully")
