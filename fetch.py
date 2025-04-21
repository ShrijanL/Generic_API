# Contains Fetch Class

from typing import Dict

from sqlalchemy import select, and_, or_, asc, desc

from utils import configure_joins


class Fetch:

    def __init__(self, config: Dict):

        self.model = config.get("model", None)
        self.filters = config.get("filters", None)
        self.joins = config.get("joins", None)
        self.fields = config.get("fields", None)
        self.page_number = config.get("page_number", None)
        self.page_size = config.get("page_size", None)
        self.sort = config.get("sort", None)
        self.distinct = config.get("distinct", None)

    def check_input_field_names(self):

        if self.fields:
            for fld in self.fields:
                if not hasattr(self.model.c, fld):
                    raise Exception(
                        {"error": f"Invalid fields in fields, {fld}", "code": "GA-010"}
                    )

        if self.filters:
            for item in self.filters:
                for val in item.value:
                    if not self.model.c[item.name].type.python_type == type(val):
                        raise Exception(
                            {
                                "error": f"Invalid value for filter:[{item.name}={val}]",
                                "code": "GA-011",
                            }
                        )

        if self.sort:
            if not hasattr(self.model.c, self.sort.field):
                raise Exception(
                    {
                        "error": f"Invalid field for sort, {self.sort.field}",
                        "code": "GA-012",
                    }
                )

    def apply_filters(self):

        query = select(*(self.model.c[col] for col in self.fields))

        if not self.filters:
            return query

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
                condition = col != value
            elif operator == "gt":
                condition = col > value
            elif operator == "lt":
                condition = col < value
            elif operator == "like":
                condition = col.like(value)
            elif operator == "ilike":
                condition = col.ilike(value)

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

        if self.distinct:
            query = query.distinct()

        return query

    def apply_joins(self, query, engine):
        if self.joins:
            for join in self.joins:
                query = configure_joins(query, join, engine)

        return query

    def apply_sort(self, query):
        column = getattr(self.model.c, self.sort.field, None)

        sorted_query = query.order_by(asc(column))
        if self.sort.order_by == "desc":
            sorted_query = query.order_by(desc(column))

        return sorted_query

    def apply_pagination(self, query):

        start_index = (self.page_number - 1) * self.page_size
        paginated_query = query.offset(start_index).limit(self.page_size)

        return paginated_query

    def parse_results(self, query):
        return [dict(zip(self.fields, row)) for row in query]
