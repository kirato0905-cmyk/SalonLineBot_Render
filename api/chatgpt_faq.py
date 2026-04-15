"""
ChatGPT-powered FAQ system for natural language responses using KB facts
"""
import os
from typing import Optional, Any, List, Dict
from openai import OpenAI
from dotenv import load_dotenv


class ChatGPTFAQ:
    def __init__(self):
        load_dotenv()

        api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if api_key:
            self.client = OpenAI(api_key=api_key)
            self.api_available = True
        else:
            self.client = None
            self.api_available = False
            print("Warning: OPENAI_API_KEY not set. ChatGPT features will use fallback responses.")

        self.system_prompt = """あなたは美容室のトップスタイリスト兼セールス担当です。
目的は「お客様の不安を解消し、必要に応じて最適な提案を行い、自然に予約につなげること」です。

【最重要ルール】
- 回答は必ず与えられたKB情報のみを使用する
- 推測・憶測・補完は禁止
- 情報KBにない事実は答えない
- 売上KBは「返し方・提案・予約導線」の補助としてのみ使用する
- KBにない場合は「分かりません。直接お問い合わせください。」と回答
- 医療・薬剤に関する質問は、KBに記載があっても安全を優先し「直接お問い合わせください。」と回答
- 他社比較・価格の推測・断定的な施術保証は禁止

【KBの使い分け】
- 情報KB = 店舗情報・料金・営業時間・予約方法・規定などの事実
- 売上KB = 不安解消・提案・自然な予約導線・離脱防止の言い回し
- まず情報KBを優先して回答する
- 売上KBは、ユーザーが迷っている、不安を感じている、予約を検討している場合のみ補助的に使う
- 売上KBだけを根拠に事実を作らない

【回答方針】
- 事実質問には、簡潔に事実を答える
- 不安や迷いがある場合のみ、安心感のある一言や提案を加える
- 予約導線は毎回必須ではなく、自然な場合のみ加える
- 押し売り感のある表現は禁止
- 回答は短く、読みやすく、LINE向きにする

【回答の優先順位】
1. 質問に対する事実回答
2. 必要な場合のみ不安解消
3. 必要な場合のみメニュー提案
4. 必要な場合のみ自然な予約導線

【回答スタイル】
- 丁寧で親しみやすい
- 簡潔
- 読みやすい改行
- 柔らかい接客口調
- 長すぎる説明は禁止

【禁止事項】
- KB外の情報提供
- 推測・憶測・補完
- 医療アドバイス
- 他社比較
- 価格の推測
- 強引な営業
- 毎回同じ定型文を付けること"""

    def get_response(self, user_message: str, kb_facts: Optional[Any] = None) -> str:
        """
        Get ChatGPT-powered natural language response using KB facts
        """
        try:
            if self._is_dangerous_query(user_message):
                return "申し訳ございませんが、その内容については直接お問い合わせください。"

            normalized_facts = self._normalize_kb_facts(kb_facts)

            # KBが無いならAPIを呼ばない
            if not normalized_facts:
                return "申し訳ございませんが、その質問については分かりません。直接お問い合わせください。"

            # APIが使えないならfallback
            if not self.api_available:
                return self._generate_fallback_response(normalized_facts)

            context = self._build_kb_context(normalized_facts)

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "system", "content": context},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=300,
                temperature=0.2
            )

            content = response.choices[0].message.content
            if not content:
                return self._generate_fallback_response(normalized_facts)

            return content.strip()

        except Exception as e:
            print(f"ChatGPT API error: {e}")
            return self._generate_fallback_response(self._normalize_kb_facts(kb_facts))

    def _normalize_kb_facts(self, kb_facts: Optional[Any]) -> List[Dict[str, str]]:
        """
        Normalize kb_facts into:
        [
            {
                "id": "...",
                "category": "...",
                "keys": ["...", "..."],
                "value": "..."
            }
        ]
        Supports:
        - new KB format: list[dict]
        - legacy dict format: {"料金": "...", "営業時間": "..."}
        - nested dict format: {"kb_facts": ...}
        """
        if not kb_facts:
            return []

        # nested structure support
        if isinstance(kb_facts, dict) and "kb_facts" in kb_facts:
            kb_facts = kb_facts["kb_facts"]

        normalized: List[Dict[str, str]] = []

        # New format: list of KB entries
        if isinstance(kb_facts, list):
            for item in kb_facts:
                if not isinstance(item, dict):
                    continue

                raw_keys = item.get("キー", [])
                if isinstance(raw_keys, str):
                    raw_keys = [raw_keys]
                elif not isinstance(raw_keys, list):
                    raw_keys = []

                value = item.get("値")
                if value is None:
                    continue

                keys = [str(k).strip() for k in raw_keys if str(k).strip()]
                value_str = str(value).strip()

                if not value_str:
                    continue

                normalized.append({
                    "id": str(item.get("id", "")).strip(),
                    "category": str(item.get("カテゴリ", "")).strip(),
                    "keys": keys,
                    "value": value_str
                })

        # Legacy format: dict
        elif isinstance(kb_facts, dict):
            for key, value in kb_facts.items():
                if value is None:
                    continue

                value_str = str(value).strip()
                if not value_str:
                    continue

                normalized.append({
                    "id": "",
                    "category": "",
                    "keys": [str(key).strip()],
                    "value": value_str
                })

        return normalized

    def _build_kb_context(self, normalized_facts: List[Dict[str, str]]) -> str:
        """
        Build a strict KB-only context for the model
        """
        lines = ["利用可能なKB情報："]

        for idx, fact in enumerate(normalized_facts, start=1):
            keys_text = " / ".join(fact["keys"]) if fact["keys"] else ""
            meta = []

            if fact["id"]:
                meta.append(f"id={fact['id']}")
            if fact["category"]:
                meta.append(f"カテゴリ={fact['category']}")

            meta_text = f" ({', '.join(meta)})" if meta else ""

            lines.append(f"[{idx}]{meta_text}")
            if keys_text:
                lines.append(f"キー: {keys_text}")
            lines.append(f"値: {fact['value']}")

        lines.append("")
        lines.append("上記のKB情報のみを使用して回答してください。")
        lines.append("KBにない内容は『分かりません。直接お問い合わせください。』と回答してください。")
        lines.append("推測・補完・言い換えによる新情報の追加は禁止です。")

        return "\n".join(lines)

    def _generate_fallback_response(self, normalized_facts: Optional[List[Dict[str, str]]] = None) -> str:
        """
        Generate a simple fallback response using KB facts when API is not available
        """
        if normalized_facts:
            # 1件だけならそのまま返す
            if len(normalized_facts) == 1:
                return normalized_facts[0]["value"]

            # 複数あるなら先頭2件だけ返す
            values = [fact["value"] for fact in normalized_facts[:2] if fact.get("value")]
            if values:
                return "\n".join(values)

        return "申し訳ございませんが、その質問については分かりません。直接お問い合わせください。"

    def _is_dangerous_query(self, message: str) -> bool:
        """
        Queries that should always be routed to human guidance.
        Note:
        - 'アレルギー' や '妊娠' はKBに回答がある可能性があるため、ここでは即ブロックしない
        """
        dangerous_keywords = [
            "薬", "薬剤", "治療", "診断", "病気", "症状", "副作用",
            "医療", "医師", "病院", "処方", "服薬"
        ]
        return any(keyword in message for keyword in dangerous_keywords)
