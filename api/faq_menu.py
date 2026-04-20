import logging
import re
from linebot.v3.messaging import (
    TemplateMessage,
    ButtonsTemplate,
    MessageAction,
    ReplyMessageRequest,
    TextMessage,
    MessagingApi,
    ApiClient,
    QuickReply,
    QuickReplyItem,
)

from api.unified_kb_loader import UnifiedKBLoader


def _get_loader() -> UnifiedKBLoader:
    return UnifiedKBLoader("api/data/unified_kb.json")


def _get_faq_display_question(entry: dict) -> str:
    """
    FAQ一覧に表示する質問文を取得
    優先順位:
    1. triggers.exact の先頭
    2. intent
    3. id
    """
    if not isinstance(entry, dict):
        return "よくある質問"

    triggers = entry.get("triggers", {})
    if isinstance(triggers, dict):
        exact = triggers.get("exact", [])
        if isinstance(exact, list) and exact:
            question = str(exact[0]).strip()
            if question:
                return question

    intent = str(entry.get("intent", "")).strip()
    if intent:
        return intent

    entry_id = str(entry.get("id", "")).strip()
    if entry_id:
        return entry_id

    return "よくある質問"


def _find_faq_entry_by_question(faq_entries: list, question: str):
    """
    質問文から FAQ entry を探す
    優先順位:
    1. triggers.exact 完全一致
    2. 表示用質問文との一致
    """
    if not question or not isinstance(faq_entries, list):
        return None

    normalized_question = str(question).strip()

    for entry in faq_entries:
        if not isinstance(entry, dict):
            continue

        triggers = entry.get("triggers", {})
        exact = triggers.get("exact", []) if isinstance(triggers, dict) else []
        if isinstance(exact, list):
            for q in exact:
                if str(q).strip() == normalized_question:
                    return entry

    for entry in faq_entries:
        if _get_faq_display_question(entry) == normalized_question:
            return entry

    return None


def send_faq_menu(reply_token, configuration):
    """
    FAQ一覧をQ1〜Q10形式で1メッセージ表示
    unified_kb.json の type='faq' を使用
    """
    try:
        loader = _get_loader()
        faq_list = loader.get_faq_entries()[:10]

        if not faq_list:
            raise ValueError("FAQ list is empty")

        lines = [
            "よくある質問はこちらです✨",
            "気になる番号をタップしてください！",
            "",
            ""
        ]

        for idx, faq in enumerate(faq_list, start=1):
            lines.append(f"Q{idx}. {_get_faq_display_question(faq)}")

        lines.append("")
        lines.append("")
        lines.append("📩上記以外でも、気になることがあればお気軽にメッセージください！")
        lines.append("そのままご予約もご案内できます✨")

        qr_items = [
            QuickReplyItem(action=MessageAction(label=f"Q{i}", text=f"Q{i}"))
            for i in range(1, len(faq_list) + 1)
        ]

        faq_menu_message = TextMessage(
            text="\n".join(lines),
            quick_reply=QuickReply(items=qr_items) if qr_items else None
        )

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[faq_menu_message]
                )
            )

        print(f"FAQ menu sent successfully ({len(faq_list)} items)")

    except Exception as e:
        logging.error(f"Error in send_faq_menu: {e}", exc_info=True)
        raise


def get_faq_by_number(faq_number):
    """
    FAQ番号でFAQを取得
    対応例:
    - Q1
    - q1
    - Q 1
    - q 1
    - 1
    - １
    - Q１
    - Ｑ２
    """
    try:
        if faq_number is None:
            return None

        text = str(faq_number).strip()

        # 全角 → 半角
        trans = str.maketrans({
            "Ｑ": "Q",
            "ｑ": "q",
            "０": "0",
            "１": "1",
            "２": "2",
            "３": "3",
            "４": "4",
            "５": "5",
            "６": "6",
            "７": "7",
            "８": "8",
            "９": "9",
        })
        text = text.translate(trans)

        # Q2 / q2 / Q 2 / 2 など
        match = re.fullmatch(r"(?:[Qq]\s*)?(\d+)", text)
        if not match:
            return None

        number = int(match.group(1))

        loader = _get_loader()
        faq_list = loader.get_faq_entries()

        if 1 <= number <= len(faq_list):
            return faq_list[number - 1]

        return None

    except Exception as e:
        logging.error(f"Error in get_faq_by_number: {e}", exc_info=True)
        return None


def send_faq_answer_by_item(reply_token, faq_item, configuration):
    """
    FAQ item（unified entry）から直接回答を送信
    """
    try:
        loader = _get_loader()
        answer = loader.render_response(faq_item)

        if not answer:
            answer = "申し訳ありません、そのFAQ番号は見つかりませんでした。"

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

        print(f"FAQ answer sent successfully for question: {_get_faq_display_question(faq_item)}")

    except Exception as e:
        logging.error(f"Error in send_faq_answer_by_item: {e}", exc_info=True)
        raise


def send_faq_answer(reply_token, question, configuration):
    """
    質問文から unified_kb.json の FAQ を検索して回答を送信
    """
    try:
        loader = _get_loader()
        faq_list = loader.get_faq_entries()
        faq_item = _find_faq_entry_by_question(faq_list, question)

        answer = loader.render_response(faq_item) if faq_item else ""
        if not answer:
            answer = "申し訳ありません、その質問は見つかりませんでした。"

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
