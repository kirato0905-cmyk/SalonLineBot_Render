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
        
        self.system_prompt = """あなたは美容室のトップスタイリスト兼セールス担当です。
目的は「お客様の不安を解消し、最適な提案を行い、自然に予約につなげること」です。

【最重要ルール】
- 回答は必ずKB情報のみを使用する
- 推測・憶測は禁止
- 不明な場合は「分かりません。直接お問い合わせください。」と回答
- 医療・薬剤に関する質問は「直接お問い合わせください。」と回答

【売上最大化ルール】
- 回答だけで終わらせず、自然に次の行動（予約・来店）につなげる
- ユーザーの意図に応じて「必要な場合のみ」メニュー提案を行う
- 不安（料金・時間・仕上がり）を自然に解消する
- 強引な営業は禁止

【回答構成】
① 共感・安心  
② 質問への回答（KBベース）  
③（必要な場合のみ）メニュー提案  
④ 自然な予約導線  

【回答スタイル】
- 丁寧で親しみやすい
- 簡潔
- 読みやすい改行
- 柔らかい接客口調

【禁止事項】
- KB外の情報提供
- 推測・憶測
- 医療アドバイス
- 他社比較
- 価格の推測"""
    
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
                model="gpt-4o-mini",
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
