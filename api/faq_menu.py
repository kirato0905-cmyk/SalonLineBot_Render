import os
import json
from linebot.v3.messaging import TemplateMessage, ButtonsTemplate, MessageAction, ReplyMessageRequest, TextMessage, MessagingApi, ApiClient

def send_faq_menu(reply_token, configuration):
    faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
    with open(faq_path, encoding="utf-8") as f:
        faq_list = json.load(f)
    actions = [
        MessageAction(label=faq["question"], text=f"FAQ:{faq['question']}")
        for faq in faq_list
    ]
    actions = actions[:10]  # ButtonsTemplateは最大10件
    faq_menu = TemplateMessage(
        alt_text="よくある質問一覧",
        template=ButtonsTemplate(
            text="よくある質問はこちらです：",
            actions=actions
        )
    )
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[faq_menu]
            )
        )

def send_faq_answer(reply_token, question, configuration):
    faq_path = os.path.join(os.path.dirname(__file__), "data", "faq.json")
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