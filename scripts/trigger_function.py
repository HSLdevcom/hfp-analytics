"""Test importer / analyzer backend function on different environments."""

import argparse
import json

import requests

ENV_URLS = {
    "local": "http://localhost:7072/",
    "dev": "https://hfp-analytics-importer-dev.azurewebsites.net/",
    "prod": "https://hfp-analytics-importer-prod.azurewebsites.net/"
}
LOCAL_TRIGGER_KEY = "MSrah7gr4eGE1x8wWAlX2uO6A3mT54NWG6FaO121ViAC7xOfd2net9=="

def main(function: str, environment: str, date_param: str, http: bool) -> None:
    if environment != "local":
        trigger_key = input(
            f"Type in >>> master key <<< to trigger {environment} {function}: "
        )

        if not trigger_key:
            print("Did not receive master key, exiting.")
            return

    else:
        trigger_key = LOCAL_TRIGGER_KEY

    payload = {}
    if date_param:
        payload["date"] = date_param

    functions_url = ""
    if http is False:
        functions_url = "admin/functions/"
    url = f"{ENV_URLS[environment]}{functions_url}{function}"
    print(
        f"Sending a request to trigger {environment} {function}... {ENV_URLS[environment]}"
    )

    resp = requests.post(
        url=url,
        headers={
            "Content-Type": "application/json",
            "x-functions-key": trigger_key
        },
        data=json.dumps(payload),
    )

    print(resp.status_code, resp.text)
    resp.raise_for_status()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helper script to trigger a backend function")
    parser.add_argument("function", help="function to be triggered", choices=["analyzer", "importer", "preprocess", "http-preprocess"])
    parser.add_argument("--env", help="environment to send the trigger", choices=["local", "dev", "prod"], default="local")
    parser.add_argument("--date", help="Date to use as oday with preprocessing, e.g. '2025-04-10'", default=None)
    parser.add_argument("--http", help="Use http trigger. With --http it is read as True", action="store_true")
    args = parser.parse_args()

    main(args.function, args.env, args.date, args.http)
