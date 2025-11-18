# core/mixins/get_data_mixin.py

from sqlalchemy import or_, and_
from biofilter.utils.formatter import orm_list_to_df


class GetDataMixin:
    def query(self, model_class, filters=None, limit=10, type=None):
        with self.biofilter.db.get_session() as session:
            query = session.query(model_class)

            if filters:
                for field_expr, value in filters.items():
                    if "__" in field_expr:
                        field, op = field_expr.split("__", 1)
                    else:
                        field, op = field_expr, "eq"

                    column = getattr(model_class, field)

                    if op == "eq":
                        query = query.filter(column == value)
                    elif op == "like":
                        query = query.filter(column.like(value))
                    elif op == "in":
                        query = query.filter(column.in_(value))
                    elif op == "between":
                        query = query.filter(column.between(*value))
                    else:
                        raise ValueError(f"Unsupported filter operator: {op}")

            if type == "df":
                return orm_list_to_df(query.limit(limit).all())
            else:
                return query.limit(limit).all()


"""
bf = Biofilter(db)
snps = bf.query(SNP, {"chromosome": "1"})
groups = bf.query(Group, {"name": "pathway_A"})
"""

# TODO:
# - Precisamos importar os modelos que serão utilizados
# - Adicionar mais controle sobre os filtros
# - Vamos retornar a queryset ou um dataframe?
# - Logging
# - Tests
