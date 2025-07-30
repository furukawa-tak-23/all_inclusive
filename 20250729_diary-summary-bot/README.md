# Diary Summary Bot

このリポジトリは、Dropbox上にある日記（Markdown）を要約し、Gmailで自動通知するGitHub Actionsボットです。

## 機能概要

- Dropbox内のファイル（指定日付）を検索・取得
- OpenAI APIを使って要約（gpt-3.5-turbo）
- Gmailで自動通知

## 必要なSecrets（GitHubに登録）

- `DROPBOX_TOKEN`
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`
- `OPENAI_API_KEY`
- `GMAIL_USER`
- `GMAIL_APP_PASSWORD`
- `GMAIL_TO`

## スケジュール

- 毎日午前9時（JST）に自動実行
