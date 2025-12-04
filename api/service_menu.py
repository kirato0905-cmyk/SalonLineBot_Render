import json
import logging
import os
from typing import Dict, List, Optional

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

SERVICE_MENU_FILE = os.path.join(os.path.dirname(__file__), "data", "service_menu.json")
DEFAULT_IMAGE = "https://img.icons8.com/?size=48&id=12245&format=png"


def _load_services() -> List[Dict]:
    if not os.path.exists(SERVICE_MENU_FILE):
        raise FileNotFoundError(f"service_menu.json not found: {SERVICE_MENU_FILE}")

    with open(SERVICE_MENU_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)

    services = payload.get("services")
    if not isinstance(services, list) or not services:
        raise ValueError("service_menu.json does not contain any services")
    return services


def _format_price(price: Optional[int]) -> str:
    if price is None:
        return "-"
    if isinstance(price, (int, float)):
        return f"¥{int(price):,}"
    return str(price)


def _create_service_bubble(service: Dict, fallback_id: str) -> FlexBubble:
    name = service.get("name", "サービス")
    description = service.get("description", "")
    duration = service.get("duration")
    price = service.get("price")
    image_url = service.get("image_url") or DEFAULT_IMAGE
    cta_label = service.get("cta_label") or "このメニューで予約する"
    resolved_id = service.get("id") or fallback_id

    info_rows: List[FlexBox] = []
    if duration:
        info_rows.append(
            FlexBox(
                layout="baseline",
                spacing="sm",
                contents=[
                    FlexText(text="所要時間", size="sm", color="#888888"),
                    FlexText(
                        text=f"{duration}分",
                        size="sm",
                        weight="bold",
                        color="#111111",
                    ),
                ],
            )
        )

    info_rows.append(
        FlexBox(
            layout="baseline",
            spacing="sm",
            contents=[
                FlexText(text="料金", size="sm", color="#888888"),
                FlexText(
                    text=_format_price(price),
                    size="sm",
                    weight="bold",
                    color="#111111",
                ),
            ],
        )
    )

    body_contents: List = [
        FlexText(text=name, weight="bold", size="xl", wrap=True),
        FlexText(
            text=description,
            size="sm",
            color="#666666",
            wrap=True,
            margin="sm",
        ),
        FlexSeparator(margin="md"),
        FlexBox(layout="vertical", spacing="sm", margin="md", contents=info_rows),
    ]

    return FlexBubble(
        hero=FlexImage(
            url=image_url,
            size="full",
            aspectRatio="20:13",
            aspectMode="cover",
        ),
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=body_contents,
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label=cta_label,
                        displayText=f"{name}で予約したい",
                        data=f"action=select_service&service_id={resolved_id}",
                    ),
                )
            ],
        ),
    )


def send_service_menu(reply_token, configuration) -> None:
    """Send Flex carousel that lists available services."""
    try:
        services = _load_services()
        bubbles = []
        for idx, service in enumerate(services, start=1):
            try:
                bubbles.append(_create_service_bubble(service, f"service_{idx}"))
            except Exception as bubble_error:
                logging.error(
                    f"Failed to build service bubble for index {idx}: {bubble_error}"
                )
        if not bubbles:
            raise ValueError("No valid service cards could be generated")

        if len(bubbles) == 1:
            container = bubbles[0]
        else:
            container = FlexCarousel(contents=bubbles[:10])  # Flex limit is 10 bubbles

        message = FlexMessage(
            alt_text="サービス一覧はこちら",
            contents=container,
        )

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[message])
            )
    except Exception as e:
        logging.error(f"Failed to send service menu: {e}", exc_info=True)
        raise

