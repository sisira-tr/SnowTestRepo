from pydantic import BaseModel, field_validator, model_validator
from typing import Optional, Literal, List, Union
import os


# -----------------------------
# SINGLE REPORT CONFIG
# -----------------------------
class SingleReportConfig(BaseModel):
    execution_mode: Literal["single"] = "single"

    report_name: str
    external_account_id: str
    academic_year: str
    subjects: Optional[Literal["math", "ela", "both"]] = "both"
    grade_min: Optional[int] = 0
    grade_max: Optional[int] = 12

    @field_validator("grade_max")
    @classmethod
    def validate_grade_range(cls, v, info):
        grade_min = info.data.get("grade_min")
        if grade_min is not None and v < grade_min:
            raise ValueError("grade_max must be >= grade_min")
        return v


# -----------------------------
# BATCH REPORT CONFIG
# -----------------------------
class BatchReportConfig(BaseModel):
    execution_mode: Literal["batch"] = "batch"

    excel_path: Optional[str] = None
    json_path: Optional[str] = None
    json_payload: Optional[List[dict]] = None

    @model_validator(mode="after")
    def validate_batch_source(self):
        provided = [self.excel_path, self.json_path, self.json_payload]
        if sum(x is not None for x in provided) != 1:
            raise ValueError(
                "Batch mode requires exactly ONE of: excel_path, json_path, or json_payload"
            )

        if self.excel_path and not os.path.exists(self.excel_path):
            raise ValueError(f"Excel file not found: {self.excel_path}")

        if self.json_path and not os.path.exists(self.json_path):
            raise ValueError(f"JSON file not found: {self.json_path}")

        return self


# -----------------------------
# UNION TYPE
# -----------------------------
ExecutionConfig = Union[SingleReportConfig, BatchReportConfig]