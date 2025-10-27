import psycopg2
import discord
from discord.ext import tasks
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Discord bot configuration
DISCORD_TOKEN = "your_discord_bot_token"  # Replace with your bot's token
DISCORD_CHANNEL_ID = 123456789012345678   # Replace with your channel ID

# Database configuration
DB_NAME = "ozone"
DB_USER = "postgres"
DB_PASSWORD = "your_postgres_password"
DB_HOST = "localhost"
DB_PORT = 5432

# Discord client setup
intents = discord.Intents.default()
client = discord.Client(intents=intents)


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

        # Query open reviews
        query = """
        SELECT id, "did", "reviewState", comment
        FROM moderation_subject_status
        WHERE "reviewState" = 'tools.ozone.moderation.defs#reviewOpen';
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            logging.info("No open reviews found.")
            return records

        for row in results:
            record_id, did, review_state, comment = row
            records.append({
                "id": record_id,
                "did": did,
                "reviewState": review_state,
                "comment": comment or "No comment provided"
            })

        cursor.close()
        conn.close()
        logging.info(f"Fetched {len(records)} open reviews from the database.")
    except Exception as e:
        logging.error(f"Failed to fetch open reviews from the database: {e}")

    return records


async def send_report_to_discord(report):
    """Send a new report message to the Discord channel."""
    channel = client.get_channel(DISCORD_CHANNEL_ID)
    if not channel:
        logging.error("Discord channel not found!")
        return

    try:
        embed = discord.Embed(
            title="New Ozone Review",
            description=f"**DID**: {report['did']}\n**Comment**: {report['comment']}",
            color=discord.Color.blue(),
        )
        embed.set_footer(text=f"Review ID: {report['id']}")
        await channel.send(embed=embed)
        logging.info(f"Sent review {report['id']} to Discord.")
    except Exception as e:
        logging.error(f"Failed to send review {report['id']} to Discord: {e}")


@tasks.loop(seconds=60)  # Check for new reviews every 60 seconds
async def check_new_reviews():
    """Periodic task to check for new reviews and send them to Discord."""
    new_reviews = fetch_open_reviews()
    for review in new_reviews:
        await send_report_to_discord(review)


@client.event
async def on_ready():
    """Event triggered when the bot is ready."""
    logging.info(f"Bot logged in as {client.user}")
    check_new_reviews.start()  # Start the periodic task


if __name__ == "__main__":
    try:
        client.run(DISCORD_TOKEN)
    except Exception as e:
        logging.error(f"Failed to run the Discord bot: {e}")
