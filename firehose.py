import json
import base64

MIN_SPEED = 20
MAX_SPEED = 120

def lambda_handler(event, context):
    output = []

    for record in event["records"]:
        payload = base64.b64decode(record["data"]).decode("utf-8")
        data = json.loads(payload)

        speed = data.get("speed")

        if speed is not None and MIN_SPEED <= speed <= MAX_SPEED:
            result = "Ok"

            if speed < 30:
                speed_range = "20_30"
            elif speed < 50:
                speed_range = "30_50"
            else:
                speed_range = "50_60"
        else:
            result = "Dropped"
            speed_range = "discarded"

        output_record = {
            "recordId": record["recordId"],
            "result": result,
            "data": base64.b64encode(
                (json.dumps(data) + "\n").encode("utf-8")
            ).decode("utf-8"),
            "metadata": {
                "partitionKeys": {
                    "speed_range": speed_range
                }
            }
        }

        output.append(output_record)

    return {"records": output}
