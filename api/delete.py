# Contains Delete Class

from typing import Dict
from sqlalchemy import and_, or_, asc, desc, delete, select, inspect
from .utils import raise_exception

class Delete:

    def __init__(self,config: Dict):
        self.model = config.get("model", None)
        self.filters = config.get("filters", None)

    def check_input_field_names(self):

        if self.filters:
            for item in self.filters:
                if not hasattr(self.model.c, item.name):
                    raise_exception(
                        error=f"Invalid name in filters, {item.name}", code="GA-014"
                    )
                for val in item.value:
                    if not self.model.c[item.name].type.python_type == type(val):
                        raise_exception(
                            error=f"Invalid value for filter:[{item.name}={val}]",
                            code="GA-015",
                        )

    def apply_delete_filters(self):

        id_fld = list(inspect(self.model).primary_key)[0]

        query = delete(self.model)
        ids_query = select(id_fld)

        total_condition = None
        last_logical_operation = "and"

        for filter_item in self.filters:
            operator = filter_item.operator
            field_name = filter_item.name
            value = filter_item.value
            operation = filter_item.operation

            col = self.model.c[field_name]
            condition = None

            if operator == "eq":
                condition = col == value[0]
            elif operator == "in":
                condition = col.in_(value)
            elif operator == "not":
                condition = col != value[0]
            elif operator == "gt":
                condition = col > value[0]
            elif operator == "lt":
                condition = col < value[0]
            elif operator == "like":
                condition = col.like(value[0])
            elif operator == "ilike":
                condition = col.ilike(value[0])

            if not total_condition:
                total_condition = condition
            else:
                if last_logical_operation == "or":
                    total_condition = or_(total_condition, condition)
                else:
                    total_condition = and_(total_condition, condition)

            last_logical_operation = operation

        if total_condition is not None:
            query = query.where(total_condition)
            ids_query = ids_query.where(total_condition)

        return query, ids_query