"""Test importer function locally."""
import requests

def main() -> int:
    trigger_key = input("Type in >>> master key <<< to trigger dev importer: ")

    if not trigger_key:
        print("Did not receive master key, exiting.")
        return

    print("Sending request to trigger dev importer...")

    resp = requests.post(
        url="https://hfp-analytics-importer-dev.azurewebsites.net/admin/functions/importer",
        headers={
            "Content-Type": "application/json",
            "x-functions-key": trigger_key
        },
        data="{ }"
    )
    print(resp.status_code)
    resp.raise_for_status()

if __name__ == "__main__":
    main()