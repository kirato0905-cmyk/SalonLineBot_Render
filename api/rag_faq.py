"""
RAG-FAQ system using FAISS for semantic search
New KB format compatible:
[
  {
    "id": "price",
    "カテゴリ": "料金",
    "キー": ["料金", "値段", "いくら"],
    "値": "料金はメニューにより異なります。詳細はサービスメニューをご確認ください。"
  }
]
"""

import json
import os
import re
from typing import Dict, Any, Optional, List

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


class RAGFAQ:
    def __init__(self, kb_data_path: str = "api/data/kb.json"):
        self.kb_data: List[Dict[str, Any]] = self._load_kb_data(kb_data_path)
        self.model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        self.index = None

        # FAISS検索用のフラット化済み候補
        self.search_items: List[Dict[str, Any]] = []

        self._build_faiss_index()

    def _load_kb_data(self, path: str) -> List[Dict[str, Any]]:
        """Load KB data from JSON file and return validated KB entry list."""
        try:
            possible_paths = []

            if os.path.isabs(path):
                possible_paths.append(path)
            else:
                clean_path = path.replace("api/", "")

                base_dirs = [
                    os.path.dirname(os.path.abspath(__file__)),
                    os.getcwd(),
                    os.path.join(os.getcwd(), "api"),
                ]

                for base_dir in base_dirs:
                    possible_paths.append(os.path.join(base_dir, clean_path))
                    possible_paths.append(os.path.join(base_dir, path))

                    if "kb.json" in clean_path:
                        possible_paths.append(
                            os.path.join(base_dir, clean_path.replace("kb.json", "KB.json"))
                        )
                    if "kb.json" in path:
                        possible_paths.append(
                            os.path.join(base_dir, path.replace("kb.json", "KB.json"))
                        )

            for full_path in possible_paths:
                try:
                    if not os.path.exists(full_path) or not os.path.isfile(full_path):
                        continue

                    with open(full_path, "r", encoding="utf-8") as f:
                        raw_data = json.load(f)

                    if not isinstance(raw_data, list):
                        print(f"Warning: KB file is not a list: {full_path}")
                        continue

                    validated = []
                    for item in raw_data:
                        if not isinstance(item, dict):
                            continue

                        kb_id = str(item.get("id", "")).strip()
                        category = str(item.get("カテゴリ", "その他")).strip()
                        keys = item.get("キー", [])
                        value = item.get("値", "")

                        if isinstance(keys, str):
                            keys = [keys]
                        elif not isinstance(keys, list):
                            keys = []

                        keys = [str(k).strip() for k in keys if str(k).strip()]
                        value = str(value).strip()

                        if not keys or not value:
                            continue

                        validated.append({
                            "id": kb_id,
                            "カテゴリ": category if category else "その他",
                            "キー": keys,
                            "値": value,
                        })

                    print(f"KB loaded: {len(validated)} entries from {full_path}")
                    return validated

                except (FileNotFoundError, OSError, json.JSONDecodeError):
                    continue

            print(f"Warning: Could not load KB data from {path}")
            return []

        except Exception as e:
            print(f"Error loading KB data from {path}: {e}")
            return []

    def _build_faiss_index(self):
        """Build FAISS index for semantic search."""
        if not self.kb_data:
            print("Warning: KB data is empty. FAISS index not built.")
            return

        texts = []
        self.search_items = []

        for entry in self.kb_data:
            keys = entry.get("キー", [])
            value = entry.get("値", "")
            category = entry.get("カテゴリ", "その他")
            kb_id = entry.get("id", "")

            for key in keys:
                enriched_text = self._create_embedding_text(key=key, value=value, category=category)

                texts.append(enriched_text)
                self.search_items.append({
                    "id": kb_id,
                    "カテゴリ": category,
                    "key": key,
                    "value": value,
                    "raw_entry": entry,
                })

        if not texts:
            print("Warning: No searchable KB texts found.")
            return

        embeddings = self.model.encode(texts, convert_to_numpy=True)
        embeddings = embeddings.astype("float32")

        faiss.normalize_L2(embeddings)

        dimension = embeddings.shape[1]
        self.index = faiss.IndexFlatIP(dimension)
        self.index.add(embeddings)

        print(f"FAISS index built: {len(self.search_items)} searchable keys")

    def _create_embedding_text(self, key: str, value: str, category: str) -> str:
        """Create enriched text for embedding to improve semantic retrieval."""
        base = f"カテゴリ {category} キー {key} 値 {value}"

        if any(word in key for word in ["住所", "場所", "アクセス", "最寄り"]):
            return f"店舗情報 アクセス 住所 場所 行き方 最寄り どこにある {base}"

        if any(word in key for word in ["営業時間", "定休日", "最終受付", "休み"]):
            return f"営業時間 時間 開店 閉店 受付 定休日 休み 何時まで いつ {base}"

        if any(word in key for word in ["予約", "当日予約", "予約方法", "変更", "キャンセル", "遅刻"]):
            return f"予約 予約方法 当日予約 変更 キャンセル 遅刻 時間変更 日時変更 予約したい {base}"

        if any(word in key for word in ["料金", "値段", "いくら", "指名料", "追加料金"]):
            return f"料金 値段 いくら 価格 金額 費用 指名料 追加料金 {base}"

        if any(word in key for word in ["カット", "カラー", "パーマ", "縮毛矯正", "トリートメント", "ヘッドスパ"]):
            return f"メニュー サービス カット カラー パーマ 縮毛矯正 トリートメント ヘッドスパ {base}"

        if any(word in key for word in ["支払い", "カード", "電子マネー", "領収書"]):
            return f"支払い 決済 カード 電子マネー 現金 領収書 {base}"

        if any(word in key for word in ["駐車場", "駐輪場", "wifi", "Wi-Fi", "コンセント"]):
            return f"設備 駐車場 駐輪場 Wi-Fi wifi コンセント 充電 {base}"

        if any(word in key for word in ["特典", "クーポン", "紹介割", "ポイント", "誕生日"]):
            return f"特典 クーポン 割引 紹介 ポイント 誕生日 初回 再来 {base}"

        if any(word in key for word in ["妊娠", "アレルギー", "持ち込み薬剤", "仕上がり保証"]):
            return f"注意事項 施術方針 妊娠 アレルギー 薬剤 保証 お直し {base}"

        return base

    def search(self, query: str, threshold: float = 0.35) -> Optional[Dict[str, Any]]:
        """
        Semantic search using FAISS.
        Returns one best matched KB entry.
        """
        if not query or not self.index or not self.search_items:
            return None

        # 危険領域はここで落とさず、上位側の制御に任せる
        query_embedding = self.model.encode([query], convert_to_numpy=True).astype("float32")
        faiss.normalize_L2(query_embedding)

        k = min(5, len(self.search_items))
        scores, indices = self.index.search(query_embedding, k)

        if len(indices[0]) == 0:
            return None

        best_idx = int(indices[0][0])
        best_score = float(scores[0][0])

        if best_score < threshold:
            return None

        best_item = self.search_items[best_idx]
        response = self._create_response(best_item["key"], best_item["value"], query)

        return {
            "kb_key": best_item["key"],
            "similarity_score": best_score,
            "kb_facts": [best_item["raw_entry"]],
            "category": best_item["カテゴリ"],
            "question": query,
            "processed_answer": response,
        }

    def search_origin(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Keyword-based search.
        More deterministic than FAISS for direct FAQ matching.
        """
        if not self.kb_data or not query:
            return None

        kb_items = []

        for entry in self.kb_data:
            keys = entry.get("キー", [])
            value = entry.get("値", "")
            category = entry.get("カテゴリ", "その他")
            kb_id = entry.get("id", "")

            for key in keys:
                kb_items.append({
                    "id": kb_id,
                    "カテゴリ": category,
                    "key": key,
                    "value": value,
                    "raw_entry": entry,
                })

        # 長いキーを優先して誤爆を減らす
        kb_items.sort(key=lambda item: len(item["key"]), reverse=True)

        for item in kb_items:
            key = item["key"]
            value = item["value"]

            if self._contains_key_as_standalone(query, key):
                response = self._create_response(key, value, query)
                return {
                    "kb_key": key,
                    "similarity_score": 1.0,
                    "kb_facts": [item["raw_entry"]],
                    "category": item["カテゴリ"],
                    "question": query,
                    "processed_answer": response,
                }

        return None

    def get_kb_facts(self, user_message: str) -> Optional[Dict[str, Any]]:
        """
        Main entry.
        First try exact-ish keyword match, then FAISS semantic search.
        """
        if not user_message:
            return None

        result = self.search_origin(user_message)
        if result:
            return result

        return self.search(user_message, threshold=0.35)

    def _create_response(self, key: str, value: str, query: str) -> str:
        """Create natural Japanese response based on the matched key."""
        if "店名" in key:
            return f"店名は「{value}」です。"

        if "住所" in key:
            return f"住所は「{value}」です。"

        if "電話番号" in key or "連絡先" in key:
            return f"お電話番号は「{value}」です。"

        if "アクセス" in key or "行き方" in key:
            return f"アクセスは「{value}」です。"

        if "最寄り" in key:
            return f"最寄りは「{value}」です。"

        if "営業時間" in key:
            return f"営業時間は「{value}」です。"

        if "最終受付" in key:
            return f"最終受付については「{value}」です。"

        if "定休日" in key or "休み" in key:
            return f"定休日は「{value}」です。"

        if "駐車場" in key:
            return f"駐車場については「{value}」です。"

        if "駐輪場" in key:
            return f"駐輪場については「{value}」です。"

        if "予約方法" in key or "どうやって予約" in key:
            return f"{value}"

        if "当日予約" in key or "今日いける" in key:
            return f"{value}"

        if "変更" in key:
            return f"予約変更については「{value}」です。"

        if "キャンセル" in key:
            return f"キャンセルについては「{value}」です。"

        if "遅刻" in key or "遅れそう" in key:
            return f"{value}"

        if "料金" in key or "値段" in key or "いくら" in key:
            return f"{value}"

        if "指名料" in key:
            return f"指名料は「{value}」です。"

        if "追加料金" in key:
            return f"追加料金については「{value}」です。"

        if any(word in key for word in ["カット", "カラー", "パーマ", "縮毛矯正", "トリートメント", "ヘッドスパ"]):
            return f"{value}"

        if "支払い" in key or "カード" in key:
            return f"{value}"

        if "領収書" in key:
            return f"{value}"

        if any(word in key for word in ["クーポン", "特典", "紹介割", "ポイント", "誕生日"]):
            return f"{value}"

        if "妊娠" in key or "アレルギー" in key:
            return f"{value} 直接お問い合わせください。"

        if "仕上がり保証" in key or "お直し" in key:
            return f"{value}"

        if "SNS" in key or "インスタ" in key or "Instagram" in key:
            return f"{value}"

        return value

    def _contains_key_as_standalone(self, query: str, key: str) -> bool:
        """
        Prevent broad false matches like:
        - 前髪カット vs カット
        """
        if not key:
            return False

        normalized_query = self._normalize_text(query)
        normalized_key = self._normalize_text(key)

        if normalized_key in normalized_query:
            start = normalized_query.find(normalized_key)
            if start == -1:
                return False

            if start == 0:
                return True

            prev_char = normalized_query[start - 1]
            allowed_preceding = {
                " ", "　", "、", "。", "(", "（", "[", "「", "『", "/", "-", "・", "+", "&",
                "は", "が", "を", "で", "に", "と", "へ", "も", "や", "の", "から", "より"
            }
            return prev_char in allowed_preceding

        return False

    def _normalize_text(self, text: str) -> str:
        """Light normalization for matching."""
        text = text.replace("＋", "+").replace("＆", "&")
        text = text.lower()
        text = re.sub(r"\s+", "", text)
        return text
