import json
import os
from typing import Any, Dict, List, Optional


class UnifiedKBLoader:
    def __init__(self, path: str = "api/data/unified_kb.json"):
        self.path = self._resolve_path(path)
        self.data = self._load()

    def _resolve_path(self, path: str) -> str:
        """
        Resolve unified_kb.json safely regardless of current working directory.
        Priority:
        1. absolute path
        2. paths relative to this file
        3. paths relative to current working directory
        4. paths relative to cwd/api
        """
        candidates: List[str] = []

        if os.path.isabs(path):
            candidates.append(path)
        else:
            current_file_dir = os.path.dirname(os.path.abspath(__file__))
            cwd = os.getcwd()

            normalized = path.replace("\\", "/")
            clean_path = normalized.replace("api/", "", 1) if normalized.startswith("api/") else normalized

            base_dirs = [
                current_file_dir,
                os.path.join(current_file_dir, "api"),
                cwd,
                os.path.join(cwd, "api"),
            ]

            for base_dir in base_dirs:
                candidates.append(os.path.join(base_dir, normalized))
                candidates.append(os.path.join(base_dir, clean_path))

        checked = []
        for candidate in candidates:
            normalized_candidate = os.path.normpath(candidate)
            if normalized_candidate in checked:
                continue
            checked.append(normalized_candidate)

            if os.path.exists(normalized_candidate) and os.path.isfile(normalized_candidate):
                return normalized_candidate

        raise FileNotFoundError(
            "Unified KB file not found. Checked paths:\n" + "\n".join(checked)
        )

    def _load(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("Unified KB must be a JSON object.")

        store_settings = data.get("store_settings", {})
        entries = data.get("entries", [])

        if not isinstance(store_settings, dict):
            raise ValueError("store_settings must be a JSON object.")

        if not isinstance(entries, list):
            raise ValueError("entries must be a JSON array.")

        normalized_entries = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue

            normalized_entry = self._normalize_entry(entry)
            if normalized_entry:
                normalized_entries.append(normalized_entry)

        return {
            "version": str(data.get("version", "")).strip(),
            "store_settings": store_settings,
            "entries": normalized_entries,
        }

    def _normalize_entry(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize each entry into a stable internal format.
        Required logical fields:
        - id
        - type
        - category
        - intent
        - triggers.exact
        - triggers.keywords
        - response.text
        Optional:
        - response.cta
        - priority
        - tags
        - enabled
        """
        entry_id = str(entry.get("id", "")).strip()
        entry_type = str(entry.get("type", "")).strip()
        category = str(entry.get("category", "")).strip()
        intent = str(entry.get("intent", "")).strip()

        triggers = entry.get("triggers", {})
        if not isinstance(triggers, dict):
            triggers = {}

        exact = triggers.get("exact", [])
        keywords = triggers.get("keywords", [])

        if isinstance(exact, str):
            exact = [exact]
        elif not isinstance(exact, list):
            exact = []

        if isinstance(keywords, str):
            keywords = [keywords]
        elif not isinstance(keywords, list):
            keywords = []

        exact = [str(x).strip() for x in exact if str(x).strip()]
        keywords = [str(x).strip() for x in keywords if str(x).strip()]

        response = entry.get("response", {})
        if not isinstance(response, dict):
            response = {}

        text = str(response.get("text", "")).strip()
        cta = str(response.get("cta", "")).strip()

        priority_raw = entry.get("priority", 0)
        try:
            priority = int(priority_raw)
        except (TypeError, ValueError):
            priority = 0

        tags = entry.get("tags", [])
        if isinstance(tags, str):
            tags = [tags]
        elif not isinstance(tags, list):
            tags = []
        tags = [str(tag).strip() for tag in tags if str(tag).strip()]

        enabled = entry.get("enabled", True)
        enabled = bool(enabled)

        if not entry_id:
            return None

        if not entry_type:
            return None

        if not text and not cta:
            return None

        return {
            "id": entry_id,
            "type": entry_type,
            "category": category,
            "intent": intent,
            "triggers": {
                "exact": exact,
                "keywords": keywords,
            },
            "response": {
                "text": text,
                "cta": cta,
            },
            "priority": priority,
            "tags": tags,
            "enabled": enabled,
        }

    def reload(self) -> None:
        self.data = self._load()

    def get_version(self) -> str:
        return self.data.get("version", "")

    def get_store_settings(self) -> Dict[str, Any]:
        return self.data.get("store_settings", {})

    def get_store_value(self, key: str, default: Any = "") -> Any:
        return self.get_store_settings().get(key, default)

    def get_entries(
        self,
        entry_type: Optional[str] = None,
        category: Optional[str] = None,
        enabled_only: bool = True,
    ) -> List[Dict[str, Any]]:
        entries = self.data.get("entries", [])
        results: List[Dict[str, Any]] = []

        for entry in entries:
            if enabled_only and not entry.get("enabled", True):
                continue

            if entry_type and entry.get("type") != entry_type:
                continue

            if category and entry.get("category") != category:
                continue

            results.append(entry)

        return results

    def get_faq_entries(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        return self.get_entries(entry_type="faq", enabled_only=enabled_only)

    def get_kb_entries(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        return self.get_entries(entry_type="kb", enabled_only=enabled_only)

    def get_sales_entries(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        return self.get_entries(entry_type="sales", enabled_only=enabled_only)

    def get_entry_by_id(self, entry_id: str) -> Optional[Dict[str, Any]]:
        if not entry_id:
            return None

        for entry in self.data.get("entries", []):
            if entry.get("id") == entry_id:
                return entry
        return None

    def render_text(self, text: str) -> str:
        """
        Replace {store_setting_key} placeholders using store_settings.
        """
        if not isinstance(text, str):
            return ""

        rendered = text
        store_settings = self.get_store_settings()

        for key, value in store_settings.items():
            rendered = rendered.replace(f"{{{key}}}", str(value))

        return rendered

    def render_response(self, entry: Dict[str, Any]) -> str:
        """
        Render response.text + response.cta with store_settings placeholders resolved.
        """
        if not isinstance(entry, dict):
            return ""

        response = entry.get("response", {})
        if not isinstance(response, dict):
            return ""

        text = self.render_text(str(response.get("text", "")).strip())
        cta = self.render_text(str(response.get("cta", "")).strip())

        if text and cta:
            return f"{text}\n{cta}"
        return text or cta

    def export_legacy_faq_list(self) -> List[Dict[str, str]]:
        """
        Convert unified faq entries into old faq.json compatible format:
        [
            {
                "question": "...",
                "answer": "..."
            }
        ]
        """
        faq_entries = self.get_faq_entries(enabled_only=True)
        results: List[Dict[str, str]] = []

        for entry in faq_entries:
            triggers = entry.get("triggers", {})
            exact = triggers.get("exact", []) if isinstance(triggers, dict) else []
            question = exact[0] if exact else entry.get("intent", entry.get("id", "FAQ"))
            answer = self.render_response(entry)

            results.append({
                "question": question,
                "answer": answer,
            })

        return results

    def export_legacy_kb_list(self, entry_type: str = "kb") -> List[Dict[str, Any]]:
        """
        Convert unified entries into old KB format:
        [
            {
                "id": "...",
                "カテゴリ": "...",
                "キー": [...],
                "値": "..."
            }
        ]
        """
        entries = self.get_entries(entry_type=entry_type, enabled_only=True)
        results: List[Dict[str, Any]] = []

        for entry in entries:
            triggers = entry.get("triggers", {})
            exact = triggers.get("exact", []) if isinstance(triggers, dict) else []
            keywords = triggers.get("keywords", []) if isinstance(triggers, dict) else []
            keys = []

            if isinstance(exact, list):
                keys.extend(exact)
            if isinstance(keywords, list):
                keys.extend(keywords)

            deduped_keys = []
            seen = set()
            for key in keys:
                normalized_key = str(key).strip()
                if not normalized_key or normalized_key in seen:
                    continue
                seen.add(normalized_key)
                deduped_keys.append(normalized_key)

            results.append({
                "id": entry.get("id", ""),
                "カテゴリ": entry.get("category", ""),
                "キー": deduped_keys,
                "値": self.render_response(entry),
            })

        return results
