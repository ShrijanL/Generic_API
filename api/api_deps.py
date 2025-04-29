from pydantic import ValidationError
from .api_config import CREATE_BATCH_SIZE
from .db_conn import EngineRegistry
from .fetch import Fetch
from .delete import Delete
from .payload_models import FetchPayload, SavePayload, LoginPayload, DeletePayload
from .services import handle_save_input, insert_records, create_tokens, delete_records
from .utils import (
    error_response,
    success_response,
    check_rec_to_be_updated,
    get_db_json_path,
)


def generic_fetch(payload):
    try:
        try:
            # Validate the payload using the Pydantic model
            validated_payload = FetchPayload(**payload)
        except ValidationError or ValueError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return error_response(error=error, code="GA-001")

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
        db_path = get_db_json_path()
        eng = EngineRegistry(db_name=input_db_name, config_path=db_path)

        model = eng.check_model(input_db_name=input_db_name, table_name=table_name)

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

        return success_response(data=data, message="Fetch Success")

    except Exception as e:
        return error_response(
            error=e.detail["error"], code=e.detail["code"], status_code=e.status_code
        )


def generic_save(payload):
    try:
        try:
            # Validate the payload using the Pydantic model
            validated_payload = SavePayload(**payload)
        except ValidationError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return error_response(error=error, code="GA-002")

        model_name = validated_payload.payload.modelName
        rec_id = validated_payload.payload.id
        saveInput = validated_payload.payload.saveInput

        if len(saveInput) > CREATE_BATCH_SIZE:
            return error_response(
                error=f"Only {CREATE_BATCH_SIZE} records allowed at a time.",
                code="GA-003",
            )

        input_db_name, table_name = model_name.split(".")
        db_path = get_db_json_path()
        eng = EngineRegistry(db_name=input_db_name, config_path=db_path)

        model = eng.check_model(input_db_name=input_db_name, table_name=table_name)

        action = "saved" if not rec_id else "updated"

        session = eng.get_session()

        # Check is rec with rec_id exists or not.
        if rec_id is not None:
            check_rec_to_be_updated(rec_id, session, model)

        statements = handle_save_input(model, rec_id, saveInput)

        result = insert_records(statements, session)
        if action == "updated":  # For update statement
            result = [rec_id]

        return success_response(data=result, message=f"{action} successfully")

    except Exception as e:
        return error_response(
            error=e.detail["error"], code=e.detail["code"], status_code=e.status_code
        )


def generic_delete(payload):
    try:
        try:
            # Validate the payload using the Pydantic model
            validated_payload = DeletePayload(**payload)
        except ValidationError or ValueError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return error_response(error=error, code="GA-004")

        modelName = validated_payload.payload.modelName
        filters = validated_payload.payload.filters

        input_db_name, table_name = modelName.split(".")
        db_path = get_db_json_path()
        eng = EngineRegistry(db_name=input_db_name, config_path=db_path)

        model = eng.check_model(input_db_name=input_db_name, table_name=table_name)

        delete_config = {
            "model": model,
            "filters": filters,
        }

        d = Delete(delete_config)
        d.check_input_field_names()
        delete_query, ids_query = d.apply_delete_filters()

        session = eng.get_session()
        ids_results = session.execute(ids_query).fetchall()
        if not ids_results:
            return error_response(
                error="No records to delete.", code="GA-005"
            )

        delete_records(delete_query, session)

        return success_response(data=[row[0] for row in ids_results], message="deleted successfully")

    except Exception as e:
        return error_response(
            error=e.detail["error"], code=e.detail["code"], status_code=e.status_code
        )


def generic_login(payload):

    try:
        try:
            validated_payload = LoginPayload(**payload)
        except ValidationError as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            error = f"{error_msg}{error_loc}"

            return error_response(error=error, code="GA-006")

        email = validated_payload.payload.email
        password = validated_payload.payload.password

        user_id = 1  # for now

        access_token, refresh_token = create_tokens(user_id)

        tokens = {
            "access": access_token,
            "refresh": refresh_token,
        }
        return success_response(data=tokens, message="Tokens are generated.")
    except Exception as e:
        return error_response(error=str(e), code="GA-007")
