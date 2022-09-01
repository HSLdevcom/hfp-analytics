"""Test importer function locally."""
import requests

def main() -> int:
    resp = requests.post(
        url="http://localhost:7072/admin/functions/importer",
        headers={
            "Content-Type": "application/json",
            "x-functions-key": "MSrah7gr4eGE1x8wWAlX2uO6A3mT54NWG6FaO121ViAC7xOfd2net9=="
        },
        data="{ }"
    )
    print(resp.status_code)
    resp.raise_for_status()

if __name__ == "__main__":
    main()