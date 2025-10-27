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


def find_mutual_connections(did):
    """Find mutual connections (accounts followed by DID that also follow DID back)."""
    logging.info(f"Fetching data for DID: {did}")
    follows = fetch_follows(did)
    followers = fetch_followers(did)

    mutual_connections = follows & followers
    logging.info(f"Found {len(mutual_connections)} mutual connections.")

    detailed_results = []
    for account in mutual_connections:
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
    # Replace this with the DID to scan
    DID = "did:plc:example"

    # Get access token
    get_access_token()

    # Find mutual connections
    mutual_connections = find_mutual_connections(DID)

    # Output results
    logging.info("Mutual connections:")
    for connection in mutual_connections:
        logging.info(
            f"DID: {connection['did']}, Handle: {connection['handle']}, Followers: {connection['followersCount']}, "
            f"Following: {connection['followsCount']}, Registered: {connection['createdAt']}"
        )


if __name__ == "__main__":
    main()
