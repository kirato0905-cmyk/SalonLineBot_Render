import os
import json
import logging
from linebot.v3.messaging import (
    TemplateMessage,
    ButtonsTemplate,
    MessageAction,
    ReplyMessageRequest,
    TextMessage,
    MessagingApi,
    ApiClient,
)
 
def send_faq_menu(reply_token, configuration):
    """FAQ一覧をQ1〜Q10形式で1メッセージ表示"""
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        print(f"Loading FAQ from: {faq_path}")
        
        if not os.path.exists(faq_path):
            raise FileNotFoundError(f"FAQ file not found: {faq_path}")
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
 
        if not faq_list or len(faq_list) == 0:
            raise ValueError("FAQ list is empty")

        faq_list = faq_list[:10]  # 最大10件まで
        print(f"Loaded {len(faq_list)} FAQ items")

        lines = ["よくある質問はこちらです：", ""]
        for idx, faq in enumerate(faq_list, start=1):
            lines.append(f"Q{idx}. {faq['question']}")

        lines.append("")
        lines.append("※質問番号（Q1～Q10）をお送りください。")

        faq_menu_message = TextMessage(text="\n".join(lines))
 
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[faq_menu_message]
                )
            )
        print("FAQ menu sent successfully")
    except Exception as e:
        logging.error(f"Error in send_faq_menu: {e}", exc_info=True)
        raise

def get_faq_by_number(faq_number):
    """番号でFAQを取得（Q1形式と数値の両方に対応）"""
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        if not os.path.exists(faq_path):
            return None
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
        
        # Q1, Q2 形式
        if isinstance(faq_number, str) and faq_number.upper().startswith("Q"):
            try:
                number = int(faq_number.upper().replace("Q", ""))
                if 1 <= number <= len(faq_list):
                    return faq_list[number - 1]
            except ValueError:
                return None
                
        return None
    except Exception as e:
        logging.error(f"Error in get_faq_by_number: {e}", exc_info=True)
        return None

def send_faq_answer_by_item(reply_token, faq_item, configuration):
    """FAQアイテム（questionとanswerを含む）から直接回答を送信"""
    try:
        answer = faq_item.get("answer", "申し訳ありません、その質問は見つかりませんでした。")
        
        back_button = TemplateMessage(
            alt_text="他のよくある質問も見る",
            template=ButtonsTemplate(
                text="他のよくある質問も見る場合はこちら",
                actions=[
                    MessageAction(label="よくある質問一覧へ戻る", text="よくある質問")
                ]
            )
        )
        
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[TextMessage(text=answer), back_button]
                )
            )
        print(f"FAQ answer sent successfully for question: {faq_item.get('question', 'Unknown')}")
    except Exception as e:
        logging.error(f"Error in send_faq_answer_by_item: {e}", exc_info=True)
        raise

def send_faq_answer(reply_token, question, configuration):
    """質問文からfaq.jsonを検索して回答を送信"""
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        print(f"Loading FAQ from: {faq_path} for question: {question}")
        
        if not os.path.exists(faq_path):
            raise FileNotFoundError(f"FAQ file not found: {faq_path}")
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
        
        # Get answer from faq.json by matching question exactly
        answer = next((faq["answer"] for faq in faq_list if faq["question"] == question), "申し訳ありません、その質問は見つかりませんでした。")
        
        back_button = TemplateMessage(
            alt_text="他のよくある質問も見る",
            template=ButtonsTemplate(
                text="他のよくある質問も見る場合はこちら",
                actions=[
                    MessageAction(label="よくある質問一覧へ戻る", text="よくある質問")
                ]
            )
        )
        
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[TextMessage(text=answer), back_button]
                )
            )
        print(f"FAQ answer sent successfully for question: {question}")
    except Exception as e:
        logging.error(f"Error in send_faq_answer: {e}", exc_info=True)
        raise