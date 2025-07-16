# biofilter/reports/queries/qry_template.py

import pandas as pd
from biofilter.reports.base import QryBase
# from biofilter.db.models import SeuModelo


class TemplateReport(QryBase):
    name = "qry_template"
    description = "Describe what this report does here"

    def run(self):
        # Exemplo básico de query (ajuste para seu modelo real)
        query = (
            self.session.query(
                # SeuModelo.campo1.label("coluna1"),
                # SeuModelo.campo2.label("coluna2"),
            )
            # .filter(...)
            # .join(...)
            # .order_by(...)
        )

        results = query.all()

        if not results:
            self.logger.info("No data found for qry_template.")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        return df



"""
⚠️ Instruções:

    - Renomeie a classe e o arquivo
    - Importe os modelos necessários
    - Use self.session e self.logger
    - Retorne sempre um DataFrame

"""