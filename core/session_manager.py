import os
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from core.logger import get_logger

logger = get_logger(__name__)

class SessionManager:

    def __init__(self):
        self.session = None

    def get_session(self):
        if not self.session:
            try:
                self.session = get_active_session()
            except Exception:
                self.session = None
            if self.session:
                logger.info("SessionManager: active Snowpark context session found — reusing it")
                return self.session
            
            logger.warning("SessionManager: no active Snowpark context session found — session is None")
            
            from dotenv import load_dotenv
            load_dotenv()
            
            user = os.environ.get("SNOW_USER")
            account = os.environ.get("SNOW_ACCOUNT")
            role = os.environ.get("SNOW_ROLE")

            if not user or not account:
                logger.error(
                    "get_active_session: SNOW_USER or SNOW_ACCOUNT environment variable is not set — "
                    "cannot create Snowflake session; returning None"
                )
                return None

            conf = {
                "user": user,
                "account": account,
                "authenticator": "externalbrowser",
                "role": role if role else None,
            }

            logger.info(
                "get_active_session: opening externalbrowser SSO session "
                "(account='%s', user='%s', role='%s') — browser window may open for authentication",
                account, user, role if role else "default",
            )
            try:
                self.session = Session.builder.configs(conf).create()
                logger.info(
                    "get_active_session: externalbrowser session created successfully "
                    "(account='%s', user='%s')",
                    account, user,
                )
                return self.session
            except Exception as e:
                logger.error(
                    "get_active_session: failed to create Snowflake session for account='%s', user='%s' — %s",
                    account, user, str(e)
                )

        return self.session

    def use_context(self, warehouse, database, schema):
        s = self.get_session()
        if not s:
            raise RuntimeError("Snowflake session could not be created. Check logs and environment variables.")

        s.sql(f"USE WAREHOUSE {warehouse}").collect()
        s.sql(f"USE DATABASE {database}").collect()
        s.sql(f"USE SCHEMA {schema}").collect()
        return s
