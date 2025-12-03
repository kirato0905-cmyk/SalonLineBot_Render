import os
import json
import logging
from linebot.v3.messaging import (
    TemplateMessage, CarouselTemplate, CarouselColumn, MessageAction,
    ReplyMessageRequest, TextMessage, MessagingApi, ApiClient
)

def truncate_label(label, max_length=20):
    """ラベルを最大文字数に制限（20文字以内）"""
    if len(label) <= max_length:
        return label
    return label[:max_length-1] + "…"

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
        
        # 最大10個のFAQを表示
        faq_list = faq_list[:10]
        print(f"Loaded {len(faq_list)} FAQ items")
        
        # 3件ずつ分割、各カラム3件に揃える
        columns = []
        items_per_column = 3
        
        for i in range(0, len(faq_list), items_per_column):
            column_faqs = faq_list[i:i+items_per_column]
            actions = []
            
            for faq in column_faqs:
                actions.append(
                    MessageAction(
                        label=truncate_label(faq["question"]),
                        text=f"FAQ:{faq['question']}"
                    )
                )
            
            # 足りない分はダミーで埋める
            while len(actions) < items_per_column:
                actions.append(
                    MessageAction(
                        label="（選択不可）",
                        text="-"  # 何も起きない
                    )
                )
            
            # titleとtextは必須
            columns.append(
                CarouselColumn(
                    title="よくある質問",
                    text="質問を選択してください",
                    actions=actions
                )
            )
        
        print(f"Created {len(columns)} carousel columns with {len(faq_list)} total FAQ items")
        
        faq_menu = TemplateMessage(
            alt_text="よくある質問一覧",
            template=CarouselTemplate(columns=columns)
        )
        
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[faq_menu]
                )
            )
        print("FAQ menu sent successfully")
    except Exception as e:
        logging.error(f"Error in send_faq_menu: {e}", exc_info=True)
        raise

def send_faq_answer(reply_token, question, configuration):
    try:
        faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
        print(f"Loading FAQ from: {faq_path} for question: {question}")
        
        if not os.path.exists(faq_path):
            raise FileNotFoundError(f"FAQ file not found: {faq_path}")
        
        with open(faq_path, encoding="utf-8") as f:
            faq_list = json.load(f)
        
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