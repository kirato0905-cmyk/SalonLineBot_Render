import os
import logging
import time
import threading
import json
from urllib.parse import parse_qs
from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage, TemplateMessage, ButtonsTemplate, MessageAction, QuickReply, QuickReplyItem
from linebot.v3.webhooks import MessageEvent, TextMessageContent, FollowEvent, PostbackEvent
from api.rag_faq import RAGFAQ
from api.chatgpt_faq import ChatGPTFAQ
from api.reservation_flow import ReservationFlow
from api.google_sheets_logger import GoogleSheetsLogger
from api.reminder_scheduler import reminder_scheduler
from api.faq_menu import send_faq_menu, send_faq_answer, send_faq_answer_by_item, get_faq_by_number
from api.service_menu import send_service_menu
from api.staff_intro import send_staff_intro

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
    sheets_logger = GoogleSheetsLogger()
    
    # Set LINE configuration for reservation flow
    reservation_flow.set_line_configuration(configuration)
    
    print("All modules initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize modules: {e}")
    # Set fallback values to prevent crashes
    rag_faq = None
    chatgpt_faq = None
    reservation_flow = None
    sheets_logger = None

app = FastAPI()

# Global variable to track scheduler thread
scheduler_thread = None

@app.on_event("startup")
async def startup_event():
    """Start the reminder scheduler on application startup"""
    global scheduler_thread
    
    try:
        if reminder_scheduler.enabled:
            print("Starting reminder scheduler...")
            
            # Start scheduler in a separate thread
            scheduler_thread = threading.Thread(
                target=reminder_scheduler.run_scheduler,
                daemon=True,  # Dies when main thread dies
                name="ReminderScheduler"
            )
            scheduler_thread.start()
            
            print("Reminder scheduler started successfully")
        else:
            print("Reminder scheduler is disabled")
            
    except Exception as e:
        logging.error(f"Failed to start reminder scheduler: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on application shutdown"""
    global scheduler_thread
    
    if scheduler_thread and scheduler_thread.is_alive():
        print("Stopping reminder scheduler...")
        # Note: The scheduler thread will stop when the main process exits
        # since it's marked as daemon=True

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
        logging.error(f"Webhook handle error: {e}")
        raise HTTPException(status_code=401, detail=str(e))
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    start_time = time.time()
    message_text = event.message.text.strip()
    user_id = event.source.user_id
    reply = ""
    quick_reply_items = []
    kb_category = None
    action_type = "message"
    reservation_data = None
    user_name = ""
    
    # Get user display name
    try:
        with ApiClient(configuration) as api_client:
            profile = MessagingApi(api_client).get_profile(user_id)
            user_name = profile.display_name
    except Exception as e:
        logging.warning(f"Could not fetch user profile for {user_id}: {e}")
        user_name = "Unknown"
    
    # Check if user has consented (except for consent-related messages)
    if message_text not in ["同意画面を開く", "同意する", "同意しない", "よくある質問"]:
        try:
            from api.user_consent_manager import user_consent_manager
            if not user_consent_manager.has_user_consented(user_id):
                # User hasn't consented - send consent reminder with button
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
                                text="同意画面を開く"
                            )
                        ]
                    )
                )
                
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[
                                TextMessage(text=consent_reminder),
                                consent_button
                            ]
                        )
                    )
                return
        except Exception as e:
            logging.error(f"Failed to check user consent: {e}")
    
    # Mark user as seen (for session tracking)
    try:
        from api.user_session_manager import user_session_manager
        user_session_manager.mark_user_seen(user_id)
    except Exception as e:
        logging.error(f"Failed to mark user as seen: {e}")

    try:
        # Handle consent flow
        if message_text == "同意画面を開く":
            return handle_consent_screen(user_id, user_name, event.reply_token)
        elif message_text in ["同意する", "同意しない"]:
            return handle_consent_response(user_id, user_name, message_text, event.reply_token)

        # Handle service list request
        service_menu_keywords = ["サービス一覧", "サービスメニュー", "メニューを見る", "メニュー"]
        if message_text in service_menu_keywords:
            try:
                send_service_menu(event.reply_token, configuration)
                action_type = "service_menu"
                if sheets_logger:
                    sheets_logger.log_message(
                        user_id=user_id,
                        user_message=message_text,
                        bot_response="Service menu displayed",
                        user_name=user_name,
                        message_type="flex",
                        action_type=action_type,
                        processing_time=(time.time() - start_time) * 1000
                    )
                return
            except Exception as e:
                logging.error(f"Failed to send service menu: {e}", exc_info=True)
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[TextMessage(text="現在サービス一覧を表示できません。しばらくしてから再度お試しください。")]
                        )
                    )
                return

        # Handle staff introduction (rich menu: スタッフ紹介)
        if message_text == "スタッフ紹介":
            try:
                send_staff_intro(event.reply_token, configuration)
                action_type = "staff_intro"
                if sheets_logger:
                    sheets_logger.log_message(
                        user_id=user_id,
                        user_message=message_text,
                        bot_response="Staff intro displayed",
                        user_name=user_name,
                        message_type="flex",
                        action_type=action_type,
                        processing_time=(time.time() - start_time) * 1000,
                    )
                return
            except Exception as e:
                logging.error(f"Failed to send staff intro: {e}", exc_info=True)
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[TextMessage(text="現在スタッフ紹介を表示できません。しばらくしてから再度お試しください。")]
                        )
                    )
                return

        # Handle FAQ menu and answers
        if message_text == "よくある質問":
            try:
                print(f"User {user_id} requested FAQ menu")
                send_faq_menu(event.reply_token, configuration)
                action_type = "faq_menu"
                print(f"FAQ menu sent successfully to user {user_id}")
                # Log FAQ menu access
                if sheets_logger:
                    sheets_logger.log_message(
                        user_id=user_id,
                        user_message=message_text,
                        bot_response="FAQ menu displayed",
                        user_name=user_name,
                        message_type="text",
                        action_type=action_type,
                        processing_time=(time.time() - start_time) * 1000
                    )
                return
            except Exception as e:
                logging.error(f"Failed to send FAQ menu: {e}", exc_info=True)
                # Send error message to user
                try:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message(
                            ReplyMessageRequest(
                                reply_token=event.reply_token,
                                messages=[TextMessage(text="申し訳ございませんが、よくある質問の表示中にエラーが発生しました。しばらくしてから再度お試しください。")]
                            )
                        )
                except Exception as reply_error:
                    logging.error(f"Failed to send error message: {reply_error}")
                return
        elif message_text.upper().startswith("Q"):
            # Handle FAQ input (Q1〜Q10)
            try:
                faq_item = get_faq_by_number(message_text)
                if faq_item and "answer" in faq_item:
                    send_faq_answer_by_item(event.reply_token, faq_item, configuration)
                    action_type = "faq_answer"
                    print(f"FAQ answer sent successfully for {message_text} to user {user_id}")
                    if sheets_logger:
                        sheets_logger.log_message(
                            user_id=user_id,
                            user_message=message_text,
                            bot_response=f"FAQ answer for: {faq_item['question']}",
                            user_name=user_name,
                            message_type="text",
                            action_type=action_type,
                            processing_time=(time.time() - start_time) * 1000
                        )
                    return
                else:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message(
                            ReplyMessageRequest(
                                reply_token=event.reply_token,
                                messages=[TextMessage(text="申し訳ございませんが、そのFAQ番号は見つかりませんでした。")]
                            )
                        )
                    return
            except Exception as e:
                logging.error(f"Failed to handle FAQ input: {e}", exc_info=True)
                try:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message(
                            ReplyMessageRequest(
                                reply_token=event.reply_token,
                                messages=[TextMessage(text="申し訳ございませんが、よくある質問の回答表示中にエラーが発生しました。しばらくしてから再度お試しください。")]
                            )
                        )
                except Exception as reply_error:
                    logging.error(f"Failed to send error message: {reply_error}")
                return

        # Special ping-pong test
        if message_text == "ping":
            reply = "pong"
            action_type = "ping"
        else:
            # 1. Try reservation flow first (highest priority)
            if reservation_flow:
                # Special handling: start reservation with preselected staff from staff intro
                if message_text.startswith("このスタッフで予約する:"):
                    staff_id = message_text.split(":", 1)[1].strip()
                    reservation_reply = reservation_flow.start_reservation_with_staff(user_id, staff_id)
                else:
                    reservation_reply = reservation_flow.get_response(user_id, message_text)

                if reservation_reply:
                    # Support Quick Reply: reply can be dict with "text" and "quick_reply_items"
                    if isinstance(reservation_reply, dict) and "text" in reservation_reply:
                        reply = reservation_reply["text"]
                        quick_reply_items = reservation_reply.get("quick_reply_items") or []
                    else:
                        reply = reservation_reply
                        quick_reply_items = []
                    action_type = "reservation"
                    # Try to get reservation data if available
                    if hasattr(reservation_flow, 'user_states') and user_id in reservation_flow.user_states:
                        reservation_data = reservation_flow.user_states[user_id].get('data', {})
                else:
                    # 2. Integrated RAG-FAQ + ChatGPT workflow
                    if rag_faq and chatgpt_faq:
                        # Step 1: Search KB for facts
                        kb_facts = rag_faq.get_kb_facts(message_text)
                        
                        if kb_facts:
                            # Step 2: Use KB facts with ChatGPT for natural language response
                            reply = chatgpt_faq.get_response(message_text, kb_facts)
                            kb_category = kb_facts.get('category', 'unknown')
                            action_type = "faq"
                            
                            # Log successful KB hit
                            print(f"KB hit for user {user_id}: {message_text} -> {kb_category}")
                        else:
                            # Step 3: No KB facts found - return standard "分かりません" response
                            reply = "申し訳ございませんが、その質問については分かりません。直接お電話にてお問い合わせください。"
                            action_type = "unknown"
                            
                            # Log KB miss for future enhancement
                            logging.warning(f"KB miss for user {user_id}: {message_text}")
                    else:
                        # Fallback when AI modules are not available
                        reply = "申し訳ございませんが、現在システムの初期化中です。しばらくお待ちください。"
                        action_type = "system_error"
            else:
                # Fallback when reservation flow is not available
                reply = "申し訳ございませんが、現在システムの初期化中です。しばらくお待ちください。"
                action_type = "system_error"

        # Reply (with optional Quick Reply)
        try:
            if quick_reply_items and len(quick_reply_items) <= 13:
                qr_items = [QuickReplyItem(action=MessageAction(label=item["label"], text=item["text"])) for item in quick_reply_items]
                text_message = TextMessage(text=reply, quick_reply=QuickReply(items=qr_items))
            else:
                text_message = TextMessage(text=reply)
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[text_message]
                    )
                )
        except Exception as e:
            logging.error(f"LINE reply error: {e}")
            # Log error to sheets
            if sheets_logger:
                sheets_logger.log_error(
                    user_id=user_id,
                    error_message=str(e),
                    user_name=user_name,
                    user_message=message_text,
                    bot_response="Error occurred"
                )
            return

    except Exception as e:
        logging.error(f"Message handling error: {e}")
        reply = "申し訳ございませんが、エラーが発生しました。"
        action_type = "error"
        # Log error to sheets
        if sheets_logger:
            sheets_logger.log_error(
                user_id=user_id,
                error_message=str(e),
                user_name=user_name,
                user_message=message_text,
                bot_response=reply
            )
        return

    # Log successful interaction to Google Sheets
    processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds
    
    # Debug logging
    print(f"Attempting to log interaction - sheets_logger: {sheets_logger is not None}, action_type: {action_type}")
    
    if sheets_logger:
        if action_type == "reservation":
            print(f"Logging reservation action for user {user_id}")
            sheets_logger.log_reservation_action(
                user_id=user_id,
                action=action_type,
                user_name=user_name,
                reservation_data=reservation_data,
                user_message=message_text,
                bot_response=reply
            )
            
            # Clear user state after logging for completed reservations
            if (reservation_flow and 
                hasattr(reservation_flow, 'user_states') and 
                user_id in reservation_flow.user_states and
                reservation_flow.user_states[user_id].get('step') == 'confirmation' and
                any(keyword in message_text for keyword in ['はい', '確定', 'お願い'])):
                del reservation_flow.user_states[user_id]
                print(f"Cleared user state for {user_id} after reservation confirmation")
                
        elif action_type == "faq":
            print(f"Logging FAQ interaction for user {user_id}")
            sheets_logger.log_faq_interaction(
                user_id=user_id,
                user_message=message_text,
                bot_response=reply,
                user_name=user_name,
                kb_category=kb_category,
                processing_time=processing_time
            )
        else:
            print(f"Logging general message for user {user_id}")
            sheets_logger.log_message(
                user_id=user_id,
                user_message=message_text,
                bot_response=reply,
                user_name=user_name,
                message_type="text",
                action_type=action_type,
                processing_time=processing_time
            )
    else:
        logging.warning(f"Sheets logger is None - cannot log interaction for user {user_id}")

@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    """Handle postback events such as Flex button taps."""
    start_time = time.time()
    user_id = event.source.user_id
    postback_data = event.postback.data or ""
    params = parse_qs(postback_data)
    action = params.get("action", [None])[0]
    reply_text = ""
    action_type = "postback"

    # Fetch user display name for logging
    try:
        with ApiClient(configuration) as api_client:
            profile = MessagingApi(api_client).get_profile(user_id)
            user_name = profile.display_name
    except Exception as e:
        logging.warning(f"Could not fetch user profile for postback {user_id}: {e}")
        user_name = "Unknown"

    if action == "select_service":
        service_id = params.get("service_id", [None])[0]
        if reservation_flow:
            try:
                reply_text = reservation_flow.start_reservation_with_service(user_id, service_id)
                action_type = "reservation"
            except Exception as e:
                logging.error(f"Failed to start reservation from postback: {e}", exc_info=True)
                reply_text = "申し訳ございませんが、メニューの処理中にエラーが発生しました。"
                action_type = "error"
        else:
            reply_text = "申し訳ございませんが、現在予約システムを利用できません。"
            action_type = "system_error"
    else:
        reply_text = "選択内容を処理できませんでした。"

    # Normalize reply: reservation flow may return dict with text + quick_reply_items
    if isinstance(reply_text, dict) and "text" in reply_text:
        reply_body = reply_text["text"]
        quick_reply_items = reply_text.get("quick_reply_items") or []
    else:
        reply_body = reply_text if isinstance(reply_text, str) else str(reply_text)
        quick_reply_items = []

    # Send reply
    try:
        if quick_reply_items and len(quick_reply_items) <= 13:
            qr_items = [QuickReplyItem(action=MessageAction(label=item["label"], text=item["text"])) for item in quick_reply_items]
            text_message = TextMessage(text=reply_body, quick_reply=QuickReply(items=qr_items))
        else:
            text_message = TextMessage(text=reply_body)
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[text_message]
                )
            )
    except Exception as e:
        logging.error(f"LINE reply error (postback): {e}")
        if sheets_logger:
            sheets_logger.log_error(
                user_id=user_id,
                error_message=str(e),
                user_name=user_name,
                user_message=f"[postback] {postback_data}",
                bot_response="Error occurred"
            )
        return

    # Log interaction
    processing_time = (time.time() - start_time) * 1000
    if sheets_logger:
        sheets_logger.log_message(
            user_id=user_id,
            user_message=f"[postback] {postback_data}",
            bot_response=reply_body,
            user_name=user_name,
            message_type="postback",
            action_type=action_type,
            processing_time=processing_time
        )
    else:
        logging.warning(f"Sheets logger is None - cannot log postback for user {user_id}")


@handler.add(FollowEvent)
def handle_follow(event: FollowEvent):
    """Handle when a user adds the bot as a friend"""
    user_id = event.source.user_id
    
    # Get user profile information
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            profile = line_bot_api.get_profile(user_id)
            user_name = profile.display_name
    except Exception as e:
        logging.warning(f"Could not fetch user profile for {user_id}: {e}")
        user_name = "Unknown"
    
    # Send login notification (user just added bot as friend)
    try:
        from api.notification_manager import send_user_login_notification
        send_user_login_notification(user_id, user_name)
        print(f"New user added bot as friend: {user_id} ({user_name})")
    except Exception as e:
        logging.error(f"Failed to send user login notification: {e}")
    
    # Save user data to Users sheet
    try:
        from api.google_sheets_logger import GoogleSheetsLogger
        sheets_logger = GoogleSheetsLogger()
        
        # In LINE Bot API, user_id is the same as LINE ID
        # Phone number is not available from LINE profile API
        sheets_logger.log_new_user(
            user_id=user_id, 
            display_name=user_name,
            phone_number=""  # Not available from LINE API
        )
        print(f"Saved user data to Users sheet: {user_name} ({user_id})")
    except Exception as e:
        logging.error(f"Failed to save user data to Users sheet: {e}")
    
    # Send consent button to the user
    try:
        consent_button = TemplateMessage(
            alt_text="ご利用前に同意が必要です",
            template=ButtonsTemplate(
                text="ご利用前に同意が必要です",
                actions=[
                    MessageAction(
                        label="ご利用前に同意",
                        text="同意画面を開く"
                    )
                ]
            )
        )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        consent_button
                    ]
                )
            )
    except Exception as e:
        logging.error(f"Failed to send consent button: {e}")

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

これらの内容に同意していただける場合は、「同意する」とお送りください。

同意いただけない場合は、ボットの利用を終了してください。"""

        consent_button = TemplateMessage(
            alt_text="利用規約に同意してください",
            template=ButtonsTemplate(
                text="利用規約に同意してください",
                actions=[
                    MessageAction(
                        label="同意する",
                        text="同意する"
                    ),
                    MessageAction(
                        label="同意しない",
                        text="同意しない"
                    )
                ]
            )
        )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[
                        TextMessage(text=consent_screen_message),
                        consent_button
                    ]
                )
            )
        
        print(f"Sent consent screen to user: {user_id} ({user_name})")
        
    except Exception as e:
        logging.error(f"Failed to send consent screen: {e}")

def handle_consent_response(user_id: str, user_name: str, message_text: str, reply_token: str):
    """Handle user's consent response"""
    try:
        if message_text == "同意する":
            # User agreed - send welcome message and mark as consented
            welcome_message = f"""✅ ご同意いただき、ありがとうございます！

{user_name}さん、サロンの予約システムをご利用いただけます。

以下の機能をご利用いただけます：

📅 予約作成
🔄 予約変更
❌ 予約キャンセル
❓ よくある質問

何かご質問がございましたら、お気軽にお声かけください。
"""

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[TextMessage(text=welcome_message)]
                    )
                )
            
            # Mark user as consented
            from api.user_consent_manager import user_consent_manager
            user_consent_manager.mark_user_consented(user_id)
            print(f"User consented: {user_id} ({user_name})")
            
        elif message_text == "同意しない":
            # User declined - send goodbye message
            goodbye_message = f"""承知いたしました。

{user_name}さん、ご利用規約にご同意いただけない場合は、ボットをご利用いただけません。

ご利用規約にご同意いただけるようになりましたら、いつでもお声かけください。

ありがとうございました。"""

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[TextMessage(text=goodbye_message)]
                    )
                )
            
            print(f"User declined consent: {user_id} ({user_name})")
        
    except Exception as e:
        logging.error(f"Failed to handle consent response: {e}")

