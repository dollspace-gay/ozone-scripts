import psycopg2
import requests
import logging
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_NAME = "ozone"
DB_USER = "postgres"
DB_PASSWORD = "foo"
DB_HOST = "localhost"
DB_PORT = 5432

# API configuration
API_URL = "https://bsky.social/xrpc"
ADMIN_USERNAME = "foo"
ADMIN_PASSWORD = "foo"
LABEL = "foo"
KEYWORD_PATTERN = re.compile(r"keyword", re.IGNORECASE)  # Case-insensitive regex for "keyword"
LABELER_DID = "foo"


def get_access_token(api_url, username, password):
    """Log into Ozone and retrieve an access token."""
    try:
        response = requests.post(
            f"{api_url}/com.atproto.server.createSession",
            json={"identifier": username, "password": password},
        )
        if response.status_code == 200:
            data = response.json()
            logging.info("Successfully logged into Ozone.")
            return data.get("accessJwt")
        else:
            logging.error(f"Failed to log in: {response.status_code} - {response.text}")
            exit(1)
    except Exception as e:
        logging.error(f"Error during login: {e}")
        exit(1)


def fetch_open_reviews():
    """Fetch records with 'reviewOpen' state from the moderation_subject_status table."""
    records = []
    try:
        logging.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        query = """
        SELECT id, did, "reviewState", comment
        FROM moderation_subject_status
        WHERE "reviewState" = 'tools.ozone.moderation.defs#reviewOpen';
        """
        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            record_id, did, review_state, comment = row
            records.append({
                "id": record_id,
                "did": did,
                "reviewState": review_state,
                "comment": comment
            })

        cursor.close()
        conn.close()
        logging.info(f"Fetched {len(records)} open reviews from the database.")
    except Exception as e:
        logging.error(f"Failed to fetch open reviews from the database: {e}")

    return records


def fetch_username_from_did(api_url, access_token, did):
    """Fetch the username (handle) associated with a DID using the Bluesky API."""
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(f"{api_url}/app.bsky.actor.getProfile", headers=headers, params={"actor": did})
        if response.status_code == 200:
            return response.json().get("handle")
        else:
            logging.error(f"Failed to fetch username for DID {did}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error fetching username for DID {did}: {e}")
        return None


def apply_label_to_did(conn, did, label):
    """Apply the specified label to a given DID."""
    try:
        cursor = conn.cursor()

        # Check if the label already exists
        uri = did
        check_query = """
        SELECT 1
        FROM label
        WHERE "src" = %s AND "uri" = %s AND "val" = %s;
        """
        cursor.execute(check_query, (LABELER_DID, uri, label))
        exists = cursor.fetchone() is not None
        if exists:
            logging.info(f"Label '{label}' already exists for DID {did}. Skipping.")
            return

        # Apply the label
        resolved_at = datetime.utcnow().isoformat()
        insert_query = """
        INSERT INTO label ("src", "uri", "cid", "val", "neg", "cts")
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        cid = ""  # Replace with actual CID if available
        cursor.execute(insert_query, (LABELER_DID, uri, cid, label, False, resolved_at))
        conn.commit()

        logging.info(f"Label '{label}' applied to DID {did}.")
    except Exception as e:
        logging.error(f"Failed to apply label to DID {did}: {e}")
    finally:
        cursor.close()


def process_reviews(reviews, access_token):
    """Process each open review."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        for review in reviews:
            record_id = review.get("id")
            did = review.get("did")
            comment = review.get("comment")

            if not record_id or not did:
                logging.warning(f"Skipping review with missing ID or DID: {review}")
                continue

            # Fetch the username for the DID
            username = fetch_username_from_did(API_URL, access_token, did)

            # Check if the username matches the keyword pattern
            if username and KEYWORD_PATTERN.search(username):
                logging.debug(f"Username '{username}' matches pattern. Applying label and closing review...")
                apply_label_to_did(conn, did, LABEL)

                # Mark review as closed
                resolved_at = datetime.utcnow().isoformat()
                update_query = """
                UPDATE moderation_subject_status
                SET "reviewState" = 'tools.ozone.moderation.defs#reviewClosed',
                    "lastReviewedAt" = %s,
                    "updatedAt" = %s
                WHERE id = %s;
                """
                cursor = conn.cursor()
                cursor.execute(update_query, (resolved_at, resolved_at, record_id))
                conn.commit()
                cursor.close()
                logging.info(f"Review for record ID {record_id} marked as closed.")
            else:
                logging.debug(f"Username '{username}' does not match pattern. Skipping review ID {record_id}.")
        conn.close()
    except Exception as e:
        logging.error(f"Error processing reviews: {e}")


def main():
    # Step 1: Log into Ozone and get the access token
    access_token = get_access_token(API_URL, ADMIN_USERNAME, ADMIN_PASSWORD)

    # Step 2: Fetch open reviews from the database
    reviews = fetch_open_reviews()

    # Step 3: Process each review
    logging.info("Processing open reviews...")
    process_reviews(reviews, access_token)

    logging.info("Review processing completed.")


if __name__ == "__main__":
    main()
