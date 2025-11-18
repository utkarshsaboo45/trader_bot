import os
import json

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes


API_KEY_PATH = 'configs/api_keys.json'

with open(API_KEY_PATH, 'r') as f:
    config = json.load(f)

TELEGRAM_API_KEY = config['TELEGRAM_API_KEY']


# openai.api_key = os.getenv("OPENAI_API_KEY")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! I'm your trader bot 🤖")
    await update.message.reply_text("What trades do you want to execute today? 🤖")


async def end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("See you on the next trading day 🤖")
    await update.message.reply_text("Hope I helped! 🤖")


async def chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text
    print(f"user: {user_msg}")

    reply = f"You said: {user_msg}"

    # completion = openai.ChatCompletion.create(
    #     model="gpt-4o-mini",
    #     messages=[{"role": "user", "content": user_msg}]
    # )
    # reply = completion.choices[0].message.content

    print(f"trader_bot: {reply}")

    await update.message.reply_text(reply)


app = ApplicationBuilder().token(TELEGRAM_API_KEY).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("end", end))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat))

print("Bot running... Press Ctrl+C to stop.")
app.run_polling()