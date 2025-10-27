import psycopg2
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_NAME = "ozone"
DB_USER = "postgres"
DB_PASSWORD = "your_postgres_password"
DB_HOST = "localhost"
DB_PORT = 5432

# Labeler configuration
LABELER_DID = "foo"
LABEL = "foo"  # Specify the label to check


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


def label_exists(conn, did, label):
    """Check if the label already exists for a given DID."""
    try:
        cursor = conn.cursor()
        uri = did  # Use DID directly as the URI
        query = """
        SELECT 1
        FROM label
        WHERE "src" = %s AND "uri" = %s AND "val" = %s;
        """
        cursor.execute(query, (LABELER_DID, uri, label))
        exists = cursor.fetchone() is not None
        cursor.close()
        logging.debug(f"Label existence check for DID {did}, label '{label}': {exists}")
        return exists
    except Exception as e:
        logging.error(f"Error checking label for DID {did}: {e}")
        return False


def close_review(conn, record_id):
    """Close the review by updating its reviewState to 'reviewClosed'."""
    try:
        resolved_at = datetime.utcnow().isoformat()
        cursor = conn.cursor()
        query = """
        UPDATE moderation_subject_status
        SET "reviewState" = 'tools.ozone.moderation.defs#reviewClosed',
            "lastReviewedAt" = %s,
            "updatedAt" = %s
        WHERE id = %s;
        """
        cursor.execute(query, (resolved_at, resolved_at, record_id))
        conn.commit()
        logging.info(f"Successfully closed review with record ID {record_id}.")
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to close review with record ID {record_id}: {e}")


def process_reviews():
    """Fetch open reviews and close those with the specified label."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Fetch open reviews
        open_reviews = fetch_open_reviews()

        # Process each review
        for review in open_reviews:
            record_id = review.get("id")
            did = review.get("did")

            if not record_id or not did:
                logging.warning(f"Skipping review with missing ID or DID: {review}")
                continue

            # Check if the DID already has the specified label
            if label_exists(conn, did, LABEL):
                logging.info(f"DID {did} already has label '{LABEL}'. Closing review {record_id}...")
                close_review(conn, record_id)
            else:
                logging.debug(f"DID {did} does not have label '{LABEL}'. Skipping review {record_id}.")

        conn.close()
    except Exception as e:
        logging.error(f"Error processing reviews: {e}")


if __name__ == "__main__":
    logging.info("Starting review processing...")
    process_reviews()
    logging.info("Review processing completed.")
