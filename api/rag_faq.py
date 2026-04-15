"""
RAG-FAQ system using FAISS for semantic search
Supports:
- information_kb.json
- sales_kb.json
"""

import json
import os
import re
from typing import Dict, Any, Optional, List

import faiss
from sentence_transformers import SentenceTransformer


class RAGFAQ:
    def __init__(
        self,
        information_kb_path: str = "api/data/information_kb.json",
        sales_kb_path: str = "api/data/sales_kb.json"
    ):
        self.information_kb: List[Dict[str, Any]] = self._load_kb_data(information_kb_path)
        self.sales_kb: List[Dict[str, Any]] = self._load_kb_data(sales_kb_path)

        self.model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

        self.info_index = None
        self.sales_index = None

        self.info_search_items: List[Dict[str, Any]] = []
        self.sales_search_items: List[Dict[str, Any]] = []

        self._build_faiss_index()

    def _load_kb_data(self, path: str) -> List[Dict[str, Any]]:
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

            for full_path in possible_paths:
                try:
                    if not os.path.exists(full_path) or not os.path.isfile(full_path):
                        continue

                    with open(full_path, "r", encoding="utf-8") as f:
                        raw_data = json.load(f)

                    if not isinstance(raw_data, list):
                        continue

                    validated = []
                    for item in raw_data:
                        if not isinstance(item, dict):
                            continue

                        kb_id = str(item.get("id", "")).strip()
                        category = str(item.get("カテゴリ", "その他")).strip()
                        keys = item.get("キー", [])
                        value = str(item.get("値", "")).strip()

                        if isinstance(keys, str):
                            keys = [keys]
                        elif not isinstance(keys, list):
                            keys = []

                        keys = [str(k).strip() for k in keys if str(k).strip()]

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
        self.info_index, self.info_search_items = self._build_single_index(self.information_kb, "情報KB")
        self.sales_index, self.sales_search_items = self._build_single_index(self.sales_kb, "売上KB")

    def _build_single_index(self, kb_data: List[Dict[str, Any]], label: str):
        if not kb_data:
            print(f"Warning: {label} is empty. FAISS index not built.")
            return None, []

        texts = []
        search_items = []

        for entry in kb_data:
            keys = entry.get("キー", [])
            value = entry.get("値", "")
            category = entry.get("カテゴリ", "その他")
            kb_id = entry.get("id", "")

            for key in keys:
                enriched_text = self._create_embedding_text(key=key, value=value, category=category)
                texts.append(enriched_text)
                search_items.append({
                    "id": kb_id,
                    "カテゴリ": category,
                    "key": key,
                    "value": value,
                    "raw_entry": entry,
                })

        if not texts:
            return None, []

        embeddings = self.model.encode(texts, convert_to_numpy=True).astype("float32")
        faiss.normalize_L2(embeddings)

        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)
        index.add(embeddings)

        print(f"{label} FAISS index built: {len(search_items)} searchable keys")
        return index, search_items

    def _create_embedding_text(self, key: str, value: str, category: str) -> str:
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

        return base

    def _normalize_text(self, text: str) -> str:
        text = text.replace("＋", "+").replace("＆", "&")
        text = text.lower()
        text = re.sub(r"\s+", "", text)
        return text

    def _contains_key_as_standalone(self, query: str, key: str) -> bool:
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

    def _keyword_search(self, query: str, kb_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        items = []

        for entry in kb_data:
            keys = entry.get("キー", [])
            value = entry.get("値", "")
            category = entry.get("カテゴリ", "その他")
            kb_id = entry.get("id", "")

            for key in keys:
                items.append({
                    "id": kb_id,
                    "カテゴリ": category,
                    "key": key,
                    "value": value,
                    "raw_entry": entry,
                })

        items.sort(key=lambda item: len(item["key"]), reverse=True)

        for item in items:
            if self._contains_key_as_standalone(query, item["key"]):
                return item

        return None

    def _semantic_search(self, query: str, index, search_items: List[Dict[str, Any]], threshold: float = 0.35):
        if not query or not index or not search_items:
            return None

        query_embedding = self.model.encode([query], convert_to_numpy=True).astype("float32")
        faiss.normalize_L2(query_embedding)

        k = min(5, len(search_items))
        scores, indices = index.search(query_embedding, k)

        if len(indices[0]) == 0:
            return None

        best_idx = int(indices[0][0])
        best_score = float(scores[0][0])

        if best_score < threshold:
            return None

        item = search_items[best_idx].copy()
        item["similarity_score"] = best_score
        return item

    def _should_attach_sales_kb(self, query: str) -> bool:
        hints = [
            "迷", "不安", "初めて", "おすすめ", "人気", "どれがいい", "相談",
            "予約したい", "今日いける", "今から", "悩み", "決まってない"
        ]
        return any(hint in query for hint in hints)

    def get_kb_facts(self, user_message: str) -> Optional[Dict[str, Any]]:
        if not user_message:
            return None

        info_match = self._keyword_search(user_message, self.information_kb)
        if not info_match:
            info_match = self._semantic_search(user_message, self.info_index, self.info_search_items, threshold=0.35)

        if not info_match:
            return None

        kb_facts = [info_match["raw_entry"]]

        if self._should_attach_sales_kb(user_message):
            sales_match = self._keyword_search(user_message, self.sales_kb)
            if not sales_match:
                sales_match = self._semantic_search(user_message, self.sales_index, self.sales_search_items, threshold=0.35)

            if sales_match:
                kb_facts.append(sales_match["raw_entry"])

        return {
            "kb_key": info_match["key"],
            "similarity_score": float(info_match.get("similarity_score", 1.0)),
            "kb_facts": kb_facts,
            "category": info_match["カテゴリ"],
            "question": user_message,
            "processed_answer": info_match["value"],
        }
