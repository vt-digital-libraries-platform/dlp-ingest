from datetime import datetime
import os
import sys
import requests


class NoidHandler:
    def __init__(self, api_key, api_endpoint, full_url, short_url):
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.full_url = full_url
        self.short_url = short_url

    def api_mint_NOID(self):
        headers = {"x-api-key": self.api_key}
        url = os.path.join(self.api_endpoint + "mint")
        response = requests.get(url, headers=headers)
        print(f"api_mint_NOID {response.text}")
        if response.status_code == 200:
            res_message = (response.json())["message"]
            start_idx = res_message.find("New NOID: ") + len("New NOID: ")
            end_idx = res_message.find(" is created.", start_idx)
            return res_message[start_idx:end_idx]
        else:
            return None

    def api_update_NOID(self, noid, create_date):
        headers = {"x-api-key": self.api_key}
        print(f"api_update_NOID: {noid}")
        body = (
            "long_url="
            + os.path.join(self.full_url, "archive", noid)
            + "&short_url="
            + os.path.join(self.short_url, "ark:/53696", noid)
            + "&noid="
            + noid
            + "&create_date="
            + create_date
        )
        url = os.path.join(self.api_endpoint + "update")
        response = requests.post(url, data=body, headers=headers)
        print(f"api_update_NOID: {response.text}")


if __name__ == "__main__":
    response = ""
    api_key = os.getenv("API_KEY")
    api_endpoint = os.getenv("API_ENDPOINT")
    full_url = os.getenv("FULL_URL")
    short_url = os.getenv("SHORT_URL")
    if len(sys.argv) < 2:
        print(f"Usage: . ./{sys.argv[0]} mint | update <noid>")
        sys.exit(1)
    else:
        noidHandler = NoidHandler(api_key, api_endpoint, full_url, short_url)
        if sys.argv[1] == "mint":
            response = noidHandler.api_mint_NOID()
            print(f"NOID: {response}")
        elif sys.argv[1] == "update":
            if len(sys.argv) < 3:
                print(f"Usage: . ./{sys.argv[0]} update <noid>")
                sys.exit(1)
            else:
                noid = sys.argv[2]
                noidHandler.api_update_NOID(
                    noid, datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                )
        else:
            print("Invalid command")
            sys.exit(1)
