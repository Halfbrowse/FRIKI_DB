from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from database import connect_to_database

app = Flask(__name__)
CORS(app)


def normalize_url(url):
    # Modify this normalization to match how URLs are stored in your database
    return url.lower().strip()


@app.route("/check", methods=["GET"])
def check_url():
    url_to_check = request.args.get("url", "")
    conn = connect_to_database()
    c = conn.cursor()
    print("Checking URL:", url_to_check)

    # List of column names to check against
    columns = [
        "External_links",
        "Website",
        "Twitter",
        "Twitter_ID",
        "Facebook",
        "Threads",
        "YouTube",
        "YouTube_ID",
        "TikTok",
        "Instagram",
        "LinkedIn",
        "Reddit",
        "VK",
        "Telegram",
        "Substack",
        "Quora",
        "Patreon",
        "GoFundMe",
        "Paypal",
        "Twitch",
        "Mastadon",
        "Wechat",
        "QQ",
        "Douyin",
    ]

    additional_columns = "Summary, Attribution_confidence"
    url_exists = False
    summary = ""
    attribution_confidence = ""
    for column in columns:
        query = (
            f"SELECT {column}, {additional_columns} FROM Entities WHERE {column} = ?"
        )
        c.execute(query, (url_to_check,))
        result = c.fetchone()
        if result:
            url_exists = True
            summary, attribution_confidence = result[1], result[2]
            break

    conn.close()
    return (
        jsonify(
            {
                "exists": url_exists,
                "summary": summary,
                "attribution_confidence": attribution_confidence,
            }
        ),
        200,
    )


if __name__ == "__main__":
    app.run(debug=True)
