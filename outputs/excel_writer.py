import pandas as pd
from datetime import datetime


class ExcelWriter:

    def write(self, df, config):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        # file_name = f"/tmp/{config.report_name}_{ts}.xlsx"
        file_name = f"{config.report_name}_{ts}.xlsx"

        with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        return file_name
