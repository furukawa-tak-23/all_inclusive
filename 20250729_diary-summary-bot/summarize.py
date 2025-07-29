import os
import datetime
import dropbox
from openai import OpenAI
import smtplib
from email.mime.text import MIMEText

def get_target_dates():
    today = datetime.date.today()
    three_months_ago = today - datetime.timedelta(days=90)
    one_month_ago = today - datetime.timedelta(days=30)
    return [three_months_ago, one_month_ago]

def find_files_for_date(dbx, date):
    year_month = date.strftime("%Y-%m")
    day_prefix = date.strftime("%Y-%m-%d")
    folder = f"/99999999_メモ、日記/{year_month}"
    files = []
    print(dbx)
    for entry in dbx.files_list_folder(folder).entries:
        if entry.name.startswith(day_prefix) and entry.name.endswith(".md"):
            files.append(f"{folder}/{entry.name}")
    return files

def fetch_and_concatenate_files(dbx, paths):
    texts = []
    for path in paths:
        _, res = dbx.files_download(path)
        texts.append(res.content.decode("utf-8"))
    return "\n\n".join(texts)

def summarize(text):
    client = OpenAI(
        api_key = os.environ["OPENAI_API_KEY"]
    )
    response = client.responses.create(
        model="gpt-3.5-turbo",
        input=[
            {
                "role": "system",
                "content": "以下はある一日の複数のメモ・日記・感想文です。全体を見て、要点と#できたことをMarkdown形式でまとめてください。"
            },
            {
                "role": "user",
                "content": text
            }
        ]
    )
    return response.output_text

def send_email(subject, body):
    from_email = os.environ["GMAIL_USER"]
    to_email = os.environ["GMAIL_TO"]
    app_password = os.environ["GMAIL_APP_PASSWORD"]

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(from_email, app_password)
        smtp.send_message(msg)

def main():
    dbx = dropbox.Dropbox(os.environ["DROPBOX_TOKEN"])
    all_output = []

    for date in get_target_dates():
        files = find_files_for_date(dbx, date)
        if not files:
            all_output.append(f"📅 {date} の記録は見つかりませんでした。")
            continue
        text = fetch_and_concatenate_files(dbx, files)
        summary = summarize(text)
        all_output.append(f"📅 {date}\n\n{summary}")

    output_text = "\n\n---\n\n".join(all_output)
    send_email("📬 日記要約通知", output_text)

if __name__ == "__main__":
    main()
