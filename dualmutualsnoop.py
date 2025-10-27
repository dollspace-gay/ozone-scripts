import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Bluesky API configuration
API_URL = "https://bsky.social/xrpc"
USERNAME = "your_bluesky_username"
PASSWORD = "your_bluesky_app_password"

# Authentication token
access_token = None


def get_access_token():
    """Log into Bluesky and retrieve an access token."""
    global access_token
    try:
        logging.info("Logging into Bluesky...")
        response = requests.post(
            f"{API_URL}/com.atproto.server.createSession",
            json={"identifier": USERNAME, "password": PASSWORD},
        )
        if response.status_code == 200:
            data = response.json()
            access_token = data.get("accessJwt")
            logging.info("Login successful.")
        else:
            logging.error(f"Failed to log in: {response.status_code} - {response.text}")
            exit(1)
    except Exception as e:
        logging.error(f"Error during login: {e}")
        exit(1)


def fetch_follows(did):
    """Fetch the list of accounts a given DID follows."""
    follows = []
    headers = {"Authorization": f"Bearer {access_token}"}
    cursor = None

    try:
        while True:
            params = {"actor": did}
            if cursor:
                params["cursor"] = cursor

            response = requests.get(f"{API_URL}/app.bsky.graph.getFollows", headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                follows.extend([item["did"] for item in data.get("follows", [])])
                cursor = data.get("cursor")
                if not cursor:
                    break
            else:
                logging.error(f"Failed to fetch follows for {did}: {response.status_code} - {response.text}")
                break
    except Exception as e:
        logging.error(f"Error fetching follows for {did}: {e}")

    return set(follows)


def fetch_followers(did):
    """Fetch the list of accounts following a given DID."""
    followers = []
    headers = {"Authorization": f"Bearer {access_token}"}
    cursor = None

    try:
        while True:
            params = {"actor": did}
            if cursor:
                params["cursor"] = cursor

            response = requests.get(f"{API_URL}/app.bsky.graph.getFollowers", headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                followers.extend([item["did"] for item in data.get("followers", [])])
                cursor = data.get("cursor")
                if not cursor:
                    break
            else:
                logging.error(f"Failed to fetch followers for {did}: {response.status_code} - {response.text}")
                break
    except Exception as e:
        logging.error(f"Error fetching followers for {did}: {e}")

    return set(followers)


def fetch_account_details(did):
    """Fetch account details for a given DID."""
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.get(f"{API_URL}/app.bsky.actor.getProfile", headers=headers, params={"actor": did})
        if response.status_code == 200:
            data = response.json()
            return {
                "handle": data.get("handle"),
                "followersCount": data.get("followersCount"),
                "followsCount": data.get("followsCount"),
                "createdAt": data.get("createdAt"),
            }
        else:
            logging.error(f"Failed to fetch account details for {did}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error fetching account details for {did}: {e}")
        return None


def find_common_accounts(did1, did2):
    """Find accounts that follow both DID1 and DID2, and are followed back by both."""
    logging.info(f"Fetching data for DID1: {did1}")
    follows1 = fetch_follows(did1)
    followers1 = fetch_followers(did1)

    logging.info(f"Fetching data for DID2: {did2}")
    follows2 = fetch_follows(did2)
    followers2 = fetch_followers(did2)

    # Common accounts that satisfy the criteria
    mutual_accounts = (follows1 & followers1) & (follows2 & followers2)

    logging.info(f"Found {len(mutual_accounts)} common accounts.")
    detailed_results = []

    for account in mutual_accounts:
        details = fetch_account_details(account)
        if details:
            detailed_results.append(
                {
                    "did": account,
                    "handle": details["handle"],
                    "followersCount": details["followersCount"],
                    "followsCount": details["followsCount"],
                    "createdAt": details["createdAt"],
                }
            )

    return detailed_results


def main():
    # Replace these with the DIDs to compare
    DID1 = "did:plc:example1"
    DID2 = "did:plc:example2"

    # Get access token
    get_access_token()

    # Find common accounts
    common_accounts = find_common_accounts(DID1, DID2)

    # Output results
    logging.info("Common accounts:")
    for account in common_accounts:
        logging.info(
            f"DID: {account['did']}, Handle: {account['handle']}, Followers: {account['followersCount']}, "
            f"Following: {account['followsCount']}, Registered: {account['createdAt']}"
        )


if __name__ == "__main__":
    main()
