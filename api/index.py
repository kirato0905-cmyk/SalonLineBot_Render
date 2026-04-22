import os
import logging
import threading
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    TemplateMessage,
    ButtonsTemplate,
    MessageAction,
    PostbackAction,
    QuickReply,
    QuickReplyItem,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent, FollowEvent, PostbackEvent

from api.rag_faq import RAGFAQ
from api.chatgpt_faq import ChatGPTFAQ
from api.reservation_flow import ReservationFlow
from api.reminder_scheduler import reminder_scheduler
from api.faq_menu import send_faq_menu, send_faq_answer_by_item, get_faq_by_number
from api.service_menu import (
    send_service_menu,
    send_single_menu_categories,
    send_single_menu_services,
)
from api.staff_intro import send_staff_intro
from api.google_sheets_logger import get_sheets_logger
from api.user_consent_manager import user_consent_manager

load_dotenv()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    raise RuntimeError("Missing LINE credentials in environment variables.")

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# Initialize AI modules with error handling
try:
    rag_faq = RAGFAQ()
    chatgpt_faq = ChatGPTFAQ()
    reservation_flow = ReservationFlow()

    # Set LINE configuration for reservation flow
    reservation_flow.set_line_configuration(configuration)

    print("All modules initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize modules: {e}", exc_info=True)
    rag_faq = None
    chatgpt_faq = None
    reservation_flow = None

app = FastAPI()

# Global variable to track scheduler thread
scheduler_thread = None

_PROFILE_CACHE = {}
_PROFILE_CACHE_TTL_SECONDS = 3600


def get_cached_display_name(user_id: str) -> str:
    import time as _time

    cached = _PROFILE_CACHE.get(user_id)
    if cached and (_time.time() - cached["fetched_at"]) <= _PROFILE_CACHE_TTL_SECONDS:
        return cached["display_name"]

    try:
        with ApiClient(configuration) as api_client:
            profile = MessagingApi(api_client).get_profile(user_id)
            display_name = profile.display_name
            _PROFILE_CACHE[user_id] = {"display_name": display_name, "fetched_at": _time.time()}
            return display_name
    except Exception as e:
        logging.warning(f"Could not fetch user profile for {user_id}: {e}")
        return cached["display_name"] if cached else "Unknown"


@app.on_event("startup")
async def startup_event():
    """Start the reminder scheduler on application startup"""
    global scheduler_thread

    try:
        if reminder_scheduler.enabled:
            print("Starting reminder scheduler...")

            scheduler_thread = threading.Thread(
                target=reminder_scheduler.run_scheduler,
                daemon=True,
                name="ReminderScheduler",
            )
            scheduler_thread.start()

            print("Reminder scheduler started successfully")
        else:
            print("Reminder scheduler is disabled")

    except Exception as e:
        logging.error(f"Failed to start reminder scheduler: {e}", exc_info=True)


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on application shutdown"""
    global scheduler_thread

    if scheduler_thread and scheduler_thread.is_alive():
        print("Stopping reminder scheduler...")
        # daemon threadなので、メインプロセス終了時に停止


@app.get("/")
async def health():
    return {"status": "ok"}


@app.get("/api/reminder-status")
async def reminder_status():
    """Get reminder scheduler status"""
    global scheduler_thread

    status = reminder_scheduler.get_status()
    status["scheduler_thread_alive"] = scheduler_thread.is_alive() if scheduler_thread else False
    return status


@app.post("/api/callback")
async def callback(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()
    body_str = body.decode("utf-8")

    try:
        handler.handle(body_str, x_line_signature)
    except InvalidSignatureError as e:
        logging.error(f"Signature error: {e}")
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        logging.error(f"Webhook handle error: {e}", exc_info=True)
        raise HTTPException(status_code=401, detail=str(e))
    return "OK"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    message_text = event.message.text.strip()
    user_id = event.source.user_id
    reply = ""
    quick_reply_items = []
    user_name = ""

    user_name = "Unknown"

    # Check consent (except for consent-related messages)
    if message_text not in ["同意画面を開く", "同意する", "同意しない", "よくある質問"]:
        try:
            if not user_consent_manager.has_user_consented(user_id):
                user_name = get_cached_display_name(user_id)
                consent_reminder = f"""🔒 プライバシー同意が必要です

{user_name}さん、ボットをご利用いただくには、まず利用規約とプライバシーポリシーにご同意いただく必要があります。

以下のボタンをタップして、同意画面をご確認ください。"""

                consent_button = TemplateMessage(
                    alt_text="利用規約に同意してください",
                    template=ButtonsTemplate(
                        text="利用規約に同意してください",
                        actions=[
                            MessageAction(
                                label="同意画面を開く",
                                text="同意画面を開く",
                            )
                        ],
                    ),
                )

                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[
                                TextMessage(text=consent_reminder),
                                consent_button,
                            ],
                        )
                    )
                return
        except Exception as e:
            logging.error(f"Failed to check user consent: {e}", exc_info=True)

    try:
        # Consent flow
        if message_text == "同意画面を開く":
            user_name = get_cached_display_name(user_id)
            return handle_consent_screen(user_id, user_name, event.reply_token)
        elif message_text in ["同意する", "同意しない"]:
            user_name = get_cached_display_name(user_id)
            return handle_consent_response(user_id, user_name, message_text, event.reply_token)

        # Service menu
        service_menu_keywords = ["サービス一覧", "サービスメニュー", "メニューを見る", "メニュー"]
        if message_text in service_menu_keywords:
            try:
                send_service_menu(event.reply_token, configuration)
                return
            except Exception as e:
                logging.error(f"Failed to send service menu: {e}", exc_info=True)
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[
                                TextMessage(
                                    text="現在サービス一覧を表示できません。しばらくしてから再度お試しください。"
                                )
                            ],
                        )
                    )
                return

        # Staff intro
        if message_text == "スタッフ紹介":
            try:
                send_staff_intro(event.reply_token, configuration)
                return
            except Exception as e:
                logging.error(f"Failed to send staff intro: {e}", exc_info=True)
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[
                                TextMessage(
                                    text="現在スタッフ紹介を表示できません。しばらくしてから再度お試しください。"
                                )
                            ],
                        )
                    )
                return

        # FAQ menu
        if message_text == "よくある質問":
            try:
                print(f"User {user_id} requested FAQ menu")
                send_faq_menu(event.reply_token, configuration)
                print(f"FAQ menu sent successfully to user {user_id}")
                return
            except Exception as e:
                logging.error(f"Failed to send FAQ menu: {e}", exc_info=True)
                try:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message(
                            ReplyMessageRequest(
                                reply_token=event.reply_token,
                                messages=[
                                    TextMessage(
                                        text="申し訳ございませんが、よくある質問の表示中にエラーが発生しました。しばらくしてから再度お試しください。"
                                    )
                                ],
                            )
                        )
                except Exception as reply_error:
                    logging.error(f"Failed to send error message: {reply_error}", exc_info=True)
                return

        # FAQ answer by number (予約フロー中は予約側を優先)
        faq_item = None
        reservation_intent = None
        if reservation_flow:
            try:
                reservation_intent = reservation_flow.detect_intent(message_text, user_id)
            except Exception:
                reservation_intent = None

        if reservation_intent not in {"reservation", "reservation_flow", "modify", "cancel"}:
            faq_item = get_faq_by_number(message_text)
        if faq_item:
            try:
                send_faq_answer_by_item(event.reply_token, faq_item, configuration)
                print(f"FAQ answer sent successfully for {message_text} to user {user_id}")
                return
            except Exception as e:
                logging.error(f"Failed to handle FAQ input: {e}", exc_info=True)
                try:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message(
                            ReplyMessageRequest(
                                reply_token=event.reply_token,
                                messages=[
                                    TextMessage(
                                        text="申し訳ございませんが、よくある質問の回答表示中にエラーが発生しました。しばらくしてから再度お試しください。"
                                    )
                                ],
                            )
                        )
                except Exception as reply_error:
                    logging.error(f"Failed to send error message: {reply_error}", exc_info=True)
                return

        # ping-pong test
        if message_text == "ping":
            reply = "pong"
        else:
            # 1. Reservation flow first
            if reservation_flow:
                if message_text.startswith("このスタッフで予約する:"):
                    staff_id = message_text.split(":", 1)[1].strip()
                    reservation_reply = reservation_flow.start_reservation_with_staff(user_id, staff_id)
                else:
                    reservation_reply = reservation_flow.get_response(user_id, message_text)

                if reservation_reply:
                    if isinstance(reservation_reply, dict) and "text" in reservation_reply:
                        reply = reservation_reply["text"]
                        quick_reply_items = reservation_reply.get("quick_reply_items") or []
                    else:
                        reply = reservation_reply
                        quick_reply_items = []
                else:
                    # 2. RAG FAQ + ChatGPT workflow
                    if rag_faq and chatgpt_faq:
                        kb_facts = rag_faq.get_kb_facts(message_text)

                        if kb_facts:
                            reply = chatgpt_faq.get_response(message_text, kb_facts["kb_facts"])
                            print(f"KB hit for user {user_id}: {message_text} -> {kb_facts.get('category', 'unknown')}")
                        else:
                            reply = "申し訳ございませんが、その質問については分かりません。直接お電話にてお問い合わせください。"
                            logging.warning(f"KB miss for user {user_id}: {message_text}")
                    else:
                        reply = "申し訳ございませんが、現在システムの初期化中です。しばらくお待ちください。"
            else:
                reply = "申し訳ございませんが、現在システムの初期化中です。しばらくお待ちください。"

        # Reply
        try:
            if quick_reply_items and len(quick_reply_items) <= 13:
                def _qr_action(item):
                    if item.get("type") == "postback" and item.get("data"):
                        return PostbackAction(label=item["label"], data=item["data"])
                    return MessageAction(label=item["label"], text=item.get("text", item["label"]))

                qr_items = [QuickReplyItem(action=_qr_action(item)) for item in quick_reply_items]
                text_message = TextMessage(text=reply, quick_reply=QuickReply(items=qr_items))
            else:
                text_message = TextMessage(text=reply)

            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[text_message],
                    )
                )

        except Exception as e:
            logging.error(f"LINE reply error: {e}", exc_info=True)
            return

    except Exception as e:
        logging.error(f"Message handling error: {e}", exc_info=True)
        return


@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    """Handle postback events such as Flex button taps."""
    user_id = event.source.user_id
    postback_data = event.postback.data or ""
    params = parse_qs(postback_data)
    action = params.get("action", [None])[0]
    reply_text = ""

    user_name = get_cached_display_name(user_id)

    if action == "select_service":
        service_id = params.get("service_id", [None])[0]
        if reservation_flow:
            try:
                reply_text = reservation_flow.start_reservation_with_service(user_id, service_id)
            except Exception as e:
                logging.error(f"Failed to start reservation from postback: {e}", exc_info=True)
                reply_text = "申し訳ございませんが、メニューの処理中にエラーが発生しました。"
        else:
            reply_text = "申し訳ございませんが、現在予約システムを利用できません。"
    elif action == "select_featured_set":
        set_id = params.get("set_id", [None])[0]
        if reservation_flow:
            try:
                reply_text = reservation_flow.start_reservation_with_featured_set(user_id, set_id)
            except Exception as e:
                logging.error(f"Failed to start reservation from featured set postback: {e}", exc_info=True)
                reply_text = "申し訳ございませんが、セットメニューの処理中にエラーが発生しました。"
        else:
            reply_text = "申し訳ございませんが、現在予約システムを利用できません。"
    elif action in {"view_single_menu", "view_single_menu_categories"}:
        try:
            send_single_menu_categories(event.reply_token, configuration)
            return
        except Exception as e:
            logging.error(f"Failed to open single menu categories from postback: {e}", exc_info=True)
            reply_text = "申し訳ございませんが、メニューカテゴリの表示中にエラーが発生しました。"
    elif action == "view_single_menu_category":
        category_id = params.get("category_id", [None])[0]
        try:
            send_single_menu_services(event.reply_token, configuration, category_id)
            return
        except Exception as e:
            logging.error(f"Failed to open single menu list from postback: {e}", exc_info=True)
            reply_text = "申し訳ございませんが、メニュー一覧の表示中にエラーが発生しました。"
    else:
        reply_text = "選択内容を処理できませんでした。"

    if isinstance(reply_text, dict) and "text" in reply_text:
        reply_body = reply_text["text"]
        quick_reply_items = reply_text.get("quick_reply_items") or []
    else:
        reply_body = reply_text if isinstance(reply_text, str) else str(reply_text)
        quick_reply_items = []

    try:
        if quick_reply_items and len(quick_reply_items) <= 13:
            def _qr_action(item):
                if item.get("type") == "postback" and item.get("data"):
                    return PostbackAction(label=item["label"], data=item["data"])
                return MessageAction(label=item["label"], text=item.get("text", item["label"]))

            qr_items = [QuickReplyItem(action=_qr_action(item)) for item in quick_reply_items]
            text_message = TextMessage(text=reply_body, quick_reply=QuickReply(items=qr_items))
        else:
            text_message = TextMessage(text=reply_body)

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[text_message],
                )
            )
    except Exception as e:
        logging.error(f"LINE reply error (postback): {e}", exc_info=True)
        return


@handler.add(FollowEvent)
def handle_follow(event: FollowEvent):
    """Handle when a user adds the bot as a friend"""
    user_id = event.source.user_id

    user_name = get_cached_display_name(user_id)

    try:
        from api.notification_manager import send_user_login_notification
        send_user_login_notification(user_id, user_name)
        print(f"New user added bot as friend: {user_id} ({user_name})")
    except Exception as e:
        logging.error(f"Failed to send user login notification: {e}", exc_info=True)

    try:
        sheets_logger = get_sheets_logger()

        sheets_logger.log_new_user(
            user_id=user_id,
            display_name=user_name,
            phone_number="",
        )
        print(f"Saved user data to Users sheet: {user_name} ({user_id})")
    except Exception as e:
        logging.error(f"Failed to save user data to Users sheet: {e}", exc_info=True)

    try:
        consent_button = TemplateMessage(
            alt_text="ご利用前に同意が必要です",
            template=ButtonsTemplate(
                text="ご利用前に同意が必要です",
                actions=[
                    MessageAction(
                        label="ご利用前に同意",
                        text="同意画面を開く",
                    )
                ],
            ),
        )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[consent_button],
                )
            )
    except Exception as e:
        logging.error(f"Failed to send consent button: {e}", exc_info=True)


def handle_consent_screen(user_id: str, user_name: str, reply_token: str):
    """Handle consent screen display"""
    try:
        consent_screen_message = f"""📋 利用規約・プライバシーポリシー

{user_name}さん、サロンの予約システムをご利用いただき、ありがとうございます。

【利用規約】
1. 予約システムは美容室の予約管理のためのサービスです
2. 正確な情報を入力してください
3. 予約の変更・キャンセルは適切な時間内に行ってください
4. システムの不適切な利用は禁止されています

【プライバシーポリシー】
1. お客様の個人情報は予約管理のみに使用されます
2. 第三者への情報提供は行いません
3. データは適切に保護・管理されます
4. お客様の同意なく情報を利用することはありません

【データの取り扱い】
• 予約情報：日時、サービス、担当者
• 連絡先：LINE ID、表示名
• 利用履歴：予約・変更・キャンセル記録

これらの内容に同意していただける場合は、「同意する」とお送りください。"""

        consent_button = TemplateMessage(
            alt_text="利用規約に同意してください",
            template=ButtonsTemplate(
                text="利用規約に同意してください",
                actions=[
                    MessageAction(label="同意する", text="同意する"),
                    MessageAction(label="同意しない", text="同意しない"),
                ],
            ),
        )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[
                        TextMessage(text=consent_screen_message),
                        consent_button,
                    ],
                )
            )

        print(f"Sent consent screen to user: {user_id} ({user_name})")

    except Exception as e:
        logging.error(f"Failed to send consent screen: {e}", exc_info=True)


def handle_consent_response(user_id: str, user_name: str, message_text: str, reply_token: str):
    """Handle user's consent response"""
    try:
        if message_text == "同意する":
            welcome_message = f"""ご同意ありがとうございます

{user_name}さん、
すぐにご予約いただけます😊

↓下のメニューからお進みください
📅予約する

その他何かご質問がございましたら、お気軽にお声かけください✨

💡ご希望の日時がある場合は、早めのご予約がおすすめです"""

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[TextMessage(text=welcome_message)],
                    )
                )

            from api.user_consent_manager import user_consent_manager
            user_consent_manager.mark_user_consented(user_id)
            print(f"User consented: {user_id} ({user_name})")

        elif message_text == "同意しない":
            goodbye_message = f"""承知いたしました。

{user_name}さん、ご利用規約にご同意いただけない場合は、ボットをご利用いただけません。

ご利用規約にご同意いただけるようになりましたら、いつでもお声かけください。

ありがとうございました。"""

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[TextMessage(text=goodbye_message)],
                    )
                )

            print(f"User declined consent: {user_id} ({user_name})")

    except Exception as e:
        logging.error(f"Failed to handle consent response: {e}", exc_info=True)
