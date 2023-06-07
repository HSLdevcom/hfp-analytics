"""Test importer / analyzer backend function on different environments."""
import argparse
import requests

ENV_URLS = {
    "local": "http://localhost:7072/admin/functions/",
    "dev": "https://hfp-analytics-importer-dev.azurewebsites.net/admin/functions/",
    "prod": "https://hfp-analytics-importer-prod.azurewebsites.net/admin/functions/"
}
LOCAL_TRIGGER_KEY = "MSrah7gr4eGE1x8wWAlX2uO6A3mT54NWG6FaO121ViAC7xOfd2net9=="


def main(function: str, environment: str) -> None:
    if environment != "local":
        trigger_key = input(f"Type in >>> master key <<< to trigger {environment} {function}: ")

        if not trigger_key:
            print("Did not receive master key, exiting.")
            return

    else:
        trigger_key = LOCAL_TRIGGER_KEY

    print(f"Sending a request to trigger {environment} {function}... {ENV_URLS[environment]}")

    resp = requests.post(
        url=f"{ENV_URLS[environment]}{function}",
        headers={"Content-Type": "application/json", "x-functions-key": trigger_key},
        data="{ }",
    )
    print(resp.status_code)
    resp.raise_for_status()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helper script to trigger a backend function")
    parser.add_argument("function", help="function to be triggered", choices=["analyzer", "importer"])
    parser.add_argument("--env", help="environment to send the trigger", choices=["local", "dev", "prod"], default="local")
    args = parser.parse_args()

    main(args.function, args.env)
