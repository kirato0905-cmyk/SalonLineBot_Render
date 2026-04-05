"""
ChatGPT-powered FAQ system for natural language responses using KB facts
"""
import os
from openai import OpenAI
from typing import Optional
from dotenv import load_dotenv

class ChatGPTFAQ:
    def __init__(self):
        # Initialize client only if API key is available
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            self.client = OpenAI(api_key=api_key)
            self.api_available = True
        else:
            self.client = None
            self.api_available = False
            print("Warning: OPENAI_API_KEY not set. ChatGPT features will use fallback responses.")
        
        self.system_prompt = """あなたは美容室のLINE予約サポートAIです。
        
目的はユーザーの疑問を解消し、自然に予約へ導くことです。

以下のルールで回答してください。

【回答ルール】
・最初に結論を簡潔に答える
・文章は短く、LINEで読みやすくする
・専門用語は使いすぎない
・丁寧だが固すぎない、やさしい口調にする

【構成】
必ず以下の流れで回答する：
① 結論（端的に）
② 補足（必要な場合のみ）
③ 次の行動の提案（必須）

【行動提案（重要）】
必ず最後に以下のような一文を入れる：
・「ご予約もご案内できます」

【禁止】
・長すぎる説明
・一般論だけの回答
・予約や行動提案がない回答

【意図理解】
ユーザーの質問の背景（目的）を推測して回答する。
例：
・「いくら？」→料金だけでなく提案もする
・「今日いける？」→空き確認へ誘導

【誘導】
ユーザーが迷っている場合は質問で会話を続ける：
例：
「どのようなスタイルをご希望ですか？」
「お時間のご希望はありますか？」

あなたの役割は「答えること」ではなく「予約につなげること」です。"""
    
    def get_response(self, user_message: str, kb_facts: Optional[dict] = None) -> str:
        """
        Get ChatGPT-powered natural language response using KB facts
        """
        try:
            # Check for dangerous queries first
            if self._is_dangerous_query(user_message):
                return "申し訳ございませんが、その質問については分かりません。直接お問い合わせください。"
            
            # If API is not available, use fallback immediately
            if not self.api_available:
                return self._generate_fallback_response(kb_facts)
            
            # Build context from KB facts
            context = ""
            if kb_facts:
                # Handle both direct kb_facts and nested structure
                facts_dict = kb_facts.get('kb_facts', kb_facts) if isinstance(kb_facts, dict) else {}
                
                if facts_dict:
                    context = f"\n\n利用可能なKB情報：\n"
                    for key, value in facts_dict.items():
                        context += f"- {key}: {value}\n"
                    context += "\n上記のKB情報のみを使用して回答してください。"
            
            response = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": self.system_prompt + context},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"ChatGPT API error: {e}")
            # Fallback: if we have KB facts, provide a simple response
            return self._generate_fallback_response(kb_facts)
    
    def _generate_fallback_response(self, kb_facts: Optional[dict] = None) -> str:
        """Generate a fallback response using KB facts when ChatGPT API is not available"""
        if kb_facts:
            facts_dict = kb_facts.get('kb_facts', kb_facts) if isinstance(kb_facts, dict) else {}
            if facts_dict:
                # Return the first available fact as a simple response
                for key, value in facts_dict.items():
                    return f"{value}です。"
        
        return "申し訳ございませんが、その質問については分かりません。直接お問い合わせください。"
    
    def _is_dangerous_query(self, message: str) -> bool:
        """Check if query is in dangerous areas that need human guidance"""
        dangerous_keywords = [
            "薬", "薬剤", "治療", "診断", "病気", "症状", "副作用",
            "アレルギー", "妊娠", "授乳", "医療", "医師", "病院"
        ]
        
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in dangerous_keywords)
