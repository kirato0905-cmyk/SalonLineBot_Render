import os
import json
import logging
from linebot.v3.messaging import (
    TemplateMessage,
    ButtonsTemplate,
    CarouselTemplate,
    CarouselColumn,
    MessageAction,
    ReplyMessageRequest,
    TextMessage,
    MessagingApi,
    ApiClient,
)
 
def send_faq_menu(reply_token, configuration):
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        print(f"Loading FAQ from: {faq_path}")
        
        if not os.path.exists(faq_path):
            raise FileNotFoundError(f"FAQ file not found: {faq_path}")
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
 
        if not faq_list or len(faq_list) == 0:
            raise ValueError("FAQ list is empty")

        # 最大10件まで（CarouselTemplateの制限）
        faq_list = faq_list[:10]
        print(f"Loaded {len(faq_list)} FAQ items")

        # CarouselTemplate用のカラムを作成
        columns = []
        for idx, faq in enumerate(faq_list, start=1):
            question = faq.get('question', '')
            # 質問文が長い場合は切り詰める（CarouselColumnのtextは最大120文字）
            if len(question) > 100:
                question = question[:97] + "..."
            
            # FAQ番号（FAQ1, FAQ2, ...）のアクションを作成
            faq_id = f"FAQ{idx}"
            columns.append(
                CarouselColumn(
                    text=question,
                    actions=[
                        MessageAction(
                            label=faq_id,
                            text=faq_id
                        )
                    ]
                )
            )

        if not columns:
            raise ValueError("No FAQ columns could be created")

        # CarouselTemplateを作成（最大10カラム）
        carousel_template = CarouselTemplate(columns=columns)
        
        # TemplateMessageを作成
        faq_menu_message = TemplateMessage(
            alt_text="よくある質問一覧",
            template=carousel_template
        )
 
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
    """番号でFAQを取得（数値またはFAQ1形式の両方に対応）"""
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        if not os.path.exists(faq_path):
            return None
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
        
        # FAQ1, FAQ2形式の入力に対応
        if isinstance(faq_number, str) and faq_number.upper().startswith("FAQ"):
            try:
                # "FAQ1" -> 1
                number = int(faq_number.upper().replace("FAQ", ""))
                if 1 <= number <= len(faq_list):
                    return faq_list[number - 1]
            except ValueError:
                return None
        
        # 数値形式の入力にも対応（後方互換性）
        if isinstance(faq_number, int):
            if 1 <= faq_number <= len(faq_list):
                return faq_list[faq_number - 1]
        
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