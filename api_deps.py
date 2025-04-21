from pydantic import ValidationError

from api_config import CREATE_BATCH_SIZE
from db_conn import EngineController, EngineRegistry
from fetch import Fetch
from payload_models import FetchPayload, SavePayload
from services import handle_save_input, insert_records, create_tokens

controller = EngineController()


def generic_fetch(payload):
    try:
        try:
            # Validate the payload using the Pydantic model
            validated_payload = FetchPayload(**payload)
        except ValidationError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return {"error": error, "code": "GA-001"}

        validated_payload_data = validated_payload.payload

        model_name = validated_payload_data.modelName
        fields = validated_payload_data.fields
        filters = validated_payload_data.filters
        joins = validated_payload_data.joins
        pageNumber = validated_payload_data.pageNumber
        pageSize = validated_payload_data.pageSize
        sort = validated_payload_data.sort
        distinct = validated_payload_data.distinct

        input_db_name, table_name = model_name.split(".")

        eng = EngineRegistry(db_name=input_db_name, controller=controller)

        try:
            model = eng.check_model(model_name)
        except Exception as e:
            return {"error": e.args[0]["error"], "code": e.args[0]["code"]}

        fetch_config = {
            "model": model,
            "fields": fields,
            "filters": filters,
            "joins": joins,
            "page_number": pageNumber,
            "page_size": pageSize,
            "sort": sort,
            "distinct": distinct,
        }

        f = Fetch(fetch_config)
        f.check_input_field_names()
        query = f.apply_filters()
        joined_query = f.apply_joins(query, eng)
        sort_query = f.apply_sort(joined_query)
        paginated_results = f.apply_pagination(sort_query)

        session = eng.get_session()

        result = session.execute(paginated_results).fetchall()

        data = f.parse_results(result)

        return data

    except Exception as e:
        if e.args:
            return {"error": e.args[0]["error"], "code": e.args[0]["code"]}
        else:
            return {"error": str(e), "code": "GA-002"}


def generic_save(payload):
    try:
        try:
            # Validate the payload using the Pydantic model
            validated_payload = SavePayload(**payload)
        except ValidationError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return {"error": error, "code": "GA-003"}

        model_name = validated_payload.payload.modelName
        rec_id = validated_payload.payload.id
        saveInput = validated_payload.payload.saveInput

        if len(saveInput) > CREATE_BATCH_SIZE:
            return {
                "error": f"Only {CREATE_BATCH_SIZE} records allowed at a time.",
                "code": "GA-004",
            }

        input_db_name, table_name = model_name.split(".")

        eng = EngineRegistry(db_name=input_db_name, controller=controller)

        try:
            model = eng.check_model(model_name)
        except Exception as e:
            return {"error": e.args[0]["error"], "code": e.args[0]["code"]}

        action = "saved" if not rec_id else "updated"

        statements = handle_save_input(model, rec_id, saveInput)

        session = eng.get_session()
        result = insert_records(statements, session)

        return {
            "data": result,
            "message": f"{action} successfully",
        }

    except Exception as e:
        return {"error": e.args[0]["error"], "code": e.args[0]["code"]}


def generic_login(payload):
    email = payload.get("payload").get("email")
    password = payload.get("payload").get("password")

    user_id = 1  # for now

    access_token, refresh_token = create_tokens(user_id)

    return access_token, refresh_token
