from atproto import Client, models
import logging

# Configure logging for verbose output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 1: Login to Bluesky with account and app password
client = Client()
logging.info("Logging into Bluesky...")
try:
    client.login('bluesky username', 'application password')
    logging.info("Login successful.")
except Exception as e:
    logging.error(f"Login failed: {e}")
    exit(1)

# Step 2: Fetch the full list of members with pagination
list_uri = "at://did:plc:foo"
did_list = []  # List to store all fetched DIDs
cursor = None  # Start without a cursor

logging.info(f"Fetching members from list: {list_uri}")
try:
    while True:
        # Fetch a batch of members, including a cursor for the next page
        params = {'list': list_uri}
        if cursor:
            params['cursor'] = cursor  # Add the cursor if it's set

        # Fetch the response
        response = client.app.bsky.graph.get_list(params)
        
        # Print response for debugging (if needed)
        logging.debug(f"Raw response: {response}")
        
        # Access items and cursor directly from the response attributes
        members = response.items if hasattr(response, 'items') else []
        cursor = response.cursor if hasattr(response, 'cursor') else None

        # Append the fetched DIDs to the list
        did_list.extend([item.subject.did for item in members])
        logging.debug(f"Fetched {len(members)} members. Total so far: {len(did_list)}")

        # Break the loop if no more pages are available
        if not cursor:
            break

    logging.info(f"Successfully fetched {len(did_list)} DIDs from the list.")
except Exception as e:
    logging.error(f"Failed to fetch list data: {e}")
    exit(1)

# Step 3: Define a reason for reporting
reason_type = 'com.atproto.moderation.defs#reasonOther'
reason = "Right-wing account flagged for moderation."

# Step 4: Use a loop to report each DID
logging.info("Starting the reporting process...")
for did in did_list:
    try:
        # Prepare the report data for the current DID
        report_data = models.ComAtprotoModerationCreateReport.Data(
            reason_type=reason_type,
            subject=models.ComAtprotoAdminDefs.RepoRef(
                did=did,
                type="com.atproto.admin.defs#repoRef"
            ),
            reason=reason
        )
        
        # Send the report to the third-party labeler
        response = client.with_proxy(
            service_type='atproto_labeler', 
            did='did:plc:foo'
        ).com.atproto.moderation.create_report(report_data)
        
        # Log success
        logging.info(f"Successfully reported {did}: {response}")
    except Exception as e:
        # Log failure
        logging.error(f"Failed to report {did}: {e}")

logging.info("Reporting process completed.")
