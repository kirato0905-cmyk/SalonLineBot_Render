import json
import logging
import os
from typing import Dict, List, Any, Optional

from linebot.v3.messaging import (
    ApiClient,
    FlexBox,
    FlexBubble,
    FlexButton,
    FlexCarousel,
    FlexImage,
    FlexMessage,
    FlexSeparator,
    FlexText,
    MessagingApi,
    PostbackAction,
    ReplyMessageRequest,
)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "data", "config.json")
DEFAULT_IMAGE = "https://img.icons8.com/?size=48&id=12245&format=png"


def _load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"config.json not found: {CONFIG_FILE}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("config.json root must be an object")

    return payload


def _safe_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value)


def _format_price(price: Optional[int]) -> str:
    if price is None:
        return "-"
    if isinstance(price, (int, float)):
        return f"¥{int(price):,}"
    return str(price)


def _format_duration(duration: Optional[int]) -> str:
    if duration is None:
        return "-"
    if isinstance(duration, (int, float)):
        return f"{int(duration)}分"
    return str(duration)


def _load_services() -> List[Dict[str, Any]]:
    payload = _load_config()
    services_raw = payload.get("services", {})
    services: List[Dict[str, Any]] = []

    if not isinstance(services_raw, dict):
        return []

    for service_key, service_data in services_raw.items():
        if not isinstance(service_data, dict):
            continue
        if service_data.get("is_active", True) is False:
            continue
        if not service_data.get("id") or not service_data.get("name"):
            continue
        normalized = dict(service_data)
        normalized["_config_key"] = service_key
        services.append(normalized)

    services.sort(
        key=lambda s: (
            str(s.get("category", "")),
            int(s.get("display_order", 999)),
            str(s.get("name", "")),
        )
    )
    return services


def _load_categories() -> List[Dict[str, Any]]:
    payload = _load_config()
    categories = payload.get("service_categories", [])
    if not isinstance(categories, list):
        return []
    return sorted(
        [c for c in categories if isinstance(c, dict) and c.get("id") and c.get("name")],
        key=lambda c: int(c.get("display_order", 999)),
    )


def _load_featured_sets() -> List[Dict[str, Any]]:
    payload = _load_config()
    featured_sets = payload.get("featured_sets", [])
    services_by_id = {s.get("id"): s for s in _load_services()}
    valid_sets: List[Dict[str, Any]] = []

    if not isinstance(featured_sets, list):
        return []

    for featured_set in featured_sets:
        if not isinstance(featured_set, dict):
            continue
        if featured_set.get("is_active", True) is False:
            continue
        service_ids = featured_set.get("services", [])
        if not isinstance(service_ids, list) or not service_ids:
            continue
        if any(service_id not in services_by_id for service_id in service_ids):
            continue
        valid_sets.append(featured_set)

    valid_sets.sort(key=lambda x: int(x.get("display_order", 999)))
    return valid_sets


def _get_category_by_id(category_id: str) -> Optional[Dict[str, Any]]:
    for category in _load_categories():
        if str(category.get("id")) == str(category_id):
            return category
    return None


def _get_services_by_category(category_id: str) -> List[Dict[str, Any]]:
    return [service for service in _load_services() if str(service.get("category")) == str(category_id)]


def _reply_flex(reply_token: str, configuration, alt_text: str, contents) -> None:
    message = FlexMessage(alt_text=alt_text, contents=contents)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token, messages=[message])
        )


def _create_featured_set_bubble(featured_set: Dict[str, Any]) -> FlexBubble:
    name = _safe_text(featured_set.get("name"), "人気セット")
    description = _safe_text(featured_set.get("description"), "人気セットメニューです。")
    duration = featured_set.get("duration")
    price = featured_set.get("price")
    set_id = _safe_text(featured_set.get("id"), "")

    body_contents: List[Any] = [
        FlexText(text="人気セット", size="sm", color="#888888"),
        FlexText(text=name, weight="bold", size="xl", wrap=True, margin="sm"),
        FlexText(text=description, size="sm", color="#666666", wrap=True, margin="md"),
        FlexSeparator(margin="md"),
        FlexBox(
            layout="vertical",
            spacing="sm",
            margin="md",
            contents=[
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="所要時間", size="sm", color="#888888"),
                        FlexText(text=_format_duration(duration), size="sm", weight="bold", color="#111111"),
                    ],
                ),
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="料金", size="sm", color="#888888"),
                        FlexText(text=_format_price(price), size="sm", weight="bold", color="#111111"),
                    ],
                ),
            ],
        ),
    ]

    return FlexBubble(
        hero=FlexImage(url=DEFAULT_IMAGE, size="full", aspectRatio="20:13", aspectMode="cover"),
        body=FlexBox(layout="vertical", spacing="md", paddingAll="16px", contents=body_contents),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label="このセットで予約する",
                        displayText=name,
                        data=f"action=select_featured_set&set_id={set_id}",
                    ),
                )
            ],
        ),
    )


def _create_category_entry_bubble() -> FlexBubble:
    categories = _load_categories()
    lines = [f"・{category.get('name')}" for category in categories]
    return FlexBubble(
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=[
                FlexText(text="単品メニュー", weight="bold", size="xl", wrap=True),
                FlexText(text="カテゴリから選びたい方はこちら", size="sm", color="#666666", wrap=True),
                FlexSeparator(margin="md"),
                FlexText(text="\n".join(lines), size="sm", wrap=True, margin="md"),
            ],
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="secondary",
                    height="sm",
                    action=PostbackAction(
                        label="メニュー一覧",
                        displayText="メニュー一覧",
                        data="action=view_single_menu_categories",
                    ),
                )
            ],
        ),
    )


def _create_single_menu_category_bubble(category: Dict[str, Any]) -> FlexBubble:
    category_id = _safe_text(category.get("id"), "")
    category_name = _safe_text(category.get("name"), "カテゴリ")
    services = _get_services_by_category(category_id)
    preview_lines = [f"・{service.get('name')}" for service in services[:6]]
    if len(services) > 6:
        preview_lines.append("・…")

    return FlexBubble(
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=[
                FlexText(text=category_name, weight="bold", size="xl", wrap=True),
                FlexText(text="このカテゴリの単品メニュー一覧です", size="sm", color="#666666", wrap=True),
                FlexSeparator(margin="md"),
                FlexText(text="\n".join(preview_lines) if preview_lines else "メニューがありません", size="sm", wrap=True, margin="md"),
            ],
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label="メニュー一覧",
                        displayText=category_name,
                        data=f"action=view_single_menu_category&category_id={category_id}",
                    ),
                )
            ],
        ),
    )


def _create_single_service_bubble(service: Dict[str, Any], category_name: str) -> FlexBubble:
    service_id = _safe_text(service.get("id"), "")
    service_name = _safe_text(service.get("name"), "メニュー")
    description = _safe_text(service.get("description"), "")
    duration = service.get("duration")
    price = service.get("price")
    image_url = _safe_text(service.get("image_url"), DEFAULT_IMAGE) or DEFAULT_IMAGE

    body_contents: List[Any] = [
        FlexText(text=category_name, size="sm", color="#888888"),
        FlexText(text=service_name, weight="bold", size="xl", wrap=True, margin="sm"),
    ]

    if description:
        body_contents.append(FlexText(text=description, size="sm", color="#666666", wrap=True, margin="md"))

    body_contents.extend([
        FlexSeparator(margin="md"),
        FlexBox(
            layout="vertical",
            spacing="sm",
            margin="md",
            contents=[
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="所要時間", size="sm", color="#888888"),
                        FlexText(text=_format_duration(duration), size="sm", weight="bold", color="#111111"),
                    ],
                ),
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="料金", size="sm", color="#888888"),
                        FlexText(text=_format_price(price), size="sm", weight="bold", color="#111111"),
                    ],
                ),
            ],
        ),
    ])

    return FlexBubble(
        hero=FlexImage(url=image_url, size="full", aspectRatio="20:13", aspectMode="cover"),
        body=FlexBox(layout="vertical", spacing="md", paddingAll="16px", contents=body_contents),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label="このメニューで予約する",
                        displayText=service_name,
                        data=f"action=select_service&service_id={service_id}",
                    ),
                )
            ],
        ),
    )


def send_service_menu(reply_token, configuration) -> None:
    try:
        featured_sets = _load_featured_sets()
        bubbles: List[FlexBubble] = []

        for featured_set in featured_sets[:9]:
            bubbles.append(_create_featured_set_bubble(featured_set))

        bubbles.append(_create_category_entry_bubble())

        if not bubbles:
            raise ValueError("No valid service cards could be generated")

        container = bubbles[0] if len(bubbles) == 1 else FlexCarousel(contents=bubbles[:10])
        _reply_flex(reply_token, configuration, "サービス一覧はこちら", container)

    except Exception as e:
        logging.error(f"Failed to send service menu: {e}", exc_info=True)
        raise


def send_single_menu_categories(reply_token, configuration) -> None:
    try:
        categories = _load_categories()
        bubbles = [_create_single_menu_category_bubble(category) for category in categories[:10]]
        if not bubbles:
            raise ValueError("No categories available")

        container = bubbles[0] if len(bubbles) == 1 else FlexCarousel(contents=bubbles)
        _reply_flex(reply_token, configuration, "単品メニューカテゴリ一覧", container)
    except Exception as e:
        logging.error(f"Failed to send single menu categories: {e}", exc_info=True)
        raise


def send_single_menu_services(reply_token, configuration, category_id: str) -> None:
    try:
        category = _get_category_by_id(category_id)
        if not category:
            raise ValueError(f"Unknown category_id: {category_id}")

        services = _get_services_by_category(category_id)
        if not services:
            raise ValueError(f"No services found for category_id: {category_id}")

        category_name = _safe_text(category.get("name"), "単品メニュー")
        bubbles = [_create_single_service_bubble(service, category_name) for service in services[:10]]
        container = bubbles[0] if len(bubbles) == 1 else FlexCarousel(contents=bubbles)
        _reply_flex(reply_token, configuration, f"{category_name}の単品メニュー一覧", container)
    except Exception as e:
        logging.error(f"Failed to send single menu services: {e}", exc_info=True)
        raise

