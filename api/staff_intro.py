import os
import json
import logging
from typing import List, Dict, Any

from linebot.v3.messaging import (
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    FlexMessage,
    FlexCarousel,
    FlexBubble,
    FlexImage,
    FlexText,
    FlexButton,
    FlexBox,
    MessageAction,
)


def _load_staffs() -> List[Dict[str, Any]]:
    """Load staff introduction data from staffs.json."""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        staffs_file = os.path.join(current_dir, "data", "staffs.json")
        with open(staffs_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                return []
            # Filter active staff and sort by order
            active = [s for s in data if s.get("is_active", True)]
            active.sort(key=lambda s: s.get("order", 999))
            return active
    except Exception as e:
        logging.error(f"Failed to load staffs.json: {e}", exc_info=True)
        return []


def _build_staff_bubble(staff: Dict[str, Any]) -> FlexBubble:
    """Build a Flex bubble for one staff member."""
    name = staff.get("name", "")
    role = staff.get("role", "")
    image_url = staff.get("image_url", "")
    strength_styles = staff.get("strength_styles") or []
    comment = staff.get("comment", "")
    sns = staff.get("sns") or {}
    instagram = sns.get("instagram") or ""
    staff_id = staff.get("staff_id", "")

    # Hero image (required)
    hero = None
    if image_url:
        hero = FlexImage(
            url=image_url,
            size="full",
            aspectRatio="20:13",
            aspectMode="cover",
        )

    # Body contents
    body_contents: List[Any] = []

    # Role + name (required)
    title_text = f"{role} {name}" if role else name
    body_contents.append(
        FlexText(text=title_text, weight="bold", size="lg", wrap=True)
    )

    # Optional: strength styles
    if strength_styles:
        body_contents.append(
            FlexText(text="得意なスタイル", weight="bold", size="sm", margin="md")
        )
        body_contents.append(
            FlexText(
                text=" / ".join(str(s) for s in strength_styles),
                size="sm",
                wrap=True,
                margin="xs",
            )
        )

    # Comment (required)
    if comment:
        body_contents.append(
            FlexText(
                text=comment,
                size="sm",
                wrap=True,
                margin="md",
            )
        )

    # Optional: SNS (Instagram)
    if instagram:
        body_contents.append(
            FlexText(
                text=f"Instagram: {instagram}",
                size="sm",
                color="#555555",
                wrap=True,
                margin="md",
            )
        )

    body = FlexBox(layout="vertical", contents=body_contents, spacing="sm")

    # Footer: "このスタッフで予約する" button
    reserve_text = f"このスタッフで予約する:{staff_id}"
    footer = FlexBox(
        layout="vertical",
        contents=[
            FlexButton(
                style="primary",
                color="#1DB446",
                action=MessageAction(
                    label="このスタッフで予約する",
                    text=reserve_text,
                ),
            )
        ],
        spacing="sm",
        paddingAll="md",
    )

    bubble = FlexBubble(hero=hero, body=body, footer=footer)
    return bubble


def send_staff_intro(reply_token: str, configuration) -> None:
    """Send staff introduction carousel as Flex Message."""
    staffs = _load_staffs()
    if not staffs:
        logging.warning("No active staffs found in staffs.json")
        # Fallback: simple text message
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[
                        FlexMessage(
                            altText="スタッフ情報が見つかりませんでした。",
                            contents=FlexBubble(
                                body=FlexBox(
                                    layout="vertical",
                                    contents=[
                                        FlexText(
                                            text="スタッフ情報が見つかりませんでした。",
                                            wrap=True,
                                        )
                                    ],
                                )
                            ),
                        )
                    ],
                )
            )
        return

    bubbles = [_build_staff_bubble(staff) for staff in staffs]
    carousel = FlexCarousel(contents=bubbles)
    flex_message = FlexMessage(
        altText="スタッフ紹介",
        contents=carousel,
    )

    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[flex_message],
            )
        )

