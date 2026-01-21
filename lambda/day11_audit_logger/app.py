import os, datetime, json
import boto3
from decimal import Decimal

DDB_TABLE = os.environ["DDB_TABLE"]
ddb = boto3.resource("dynamodb").Table(DDB_TABLE)

def _now():
    return datetime.datetime.utcnow().isoformat() + "Z"

def _convert_floats(obj):
    """Recursively convert floats to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats(i) for i in obj]
    return obj

def handler(event, context):
    mode = event.get("mode", "init")
    execution_id = event["execution_id"]

    if mode == "init":
        item = {
            "execution_id": execution_id,
            "status": "RUNNING",
            "trigger": _convert_floats(event.get("trigger", {})),
            "inputs": _convert_floats(event.get("inputs", {})),
            "started_at_utc": _now(),
            "updated_at_utc": _now(),
        }
        ddb.put_item(Item=item)
        return {"ok": True, "execution_id": execution_id}

    updates = _convert_floats(event.get("updates", {}))
    updates["updated_at_utc"] = _now()

    expr = "SET " + ", ".join([f"#{k} = :{k}" for k in updates.keys()])
    names = {f"#{k}": k for k in updates.keys()}
    vals = {f":{k}": v for k, v in updates.items()}

    ddb.update_item(
        Key={"execution_id": execution_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals
    )
    return {"ok": True, "execution_id": execution_id, "updates": updates}
