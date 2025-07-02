import google.generativeai as genai
from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

class Gemini:
    def __init__(self):
        logger.announcement("Initializing Gemini Client...", 'info')
        try:
            genai.configure(api_key=get_secret("GOOGLE_GENAI_API_KEY"))
            self.model = genai.GenerativeModel(
                model_name='gemini-1.5-flash',
                generation_config=genai.types.GenerationConfig(
                    temperature=0.75
                ),
                safety_settings=[],
                system_instruction="""
                You are AGM's AI assistant called Ada. Here's important context about AGM:

                AGM is a leading International Securities Broker/Dealer since 1995, providing direct access to over 150 financial markets across the USA, Europe, Asia, and Latin America. We specialize in making previously inaccessible markets accessible to traders and investors.

                Key Services:
                - AGM Trader: For individual traders
                - AGM Advisor: For managed investment solutions
                - AGM Institutional: For institutional clients

                We offer:
                - Global trading platform for stocks, ETFs, options, futures, bonds, and cryptocurrencies
                - 24/7 market access
                - Professional trading tools and dashboard
                - Dedicated customer support

                Please provide accurate, professional, and helpful responses related to AGM's services, trading capabilities, and financial markets. Always maintain a professional tone and prioritize accuracy in financial information.

                YOU DO NOT OFFER INVESTMENT ADVICE, YOU ARE NOT A FINANCIAL ADVISOR.
                """
            )
            logger.announcement("Gemini Client initialized", 'success')
        except Exception as e:
            logger.error(f"Failed to initialize Gemini Client: {str(e)}")
            raise Exception("Failed to initialize Gemini Client")

    def chat(self, messages):
        try:
            logger.info(f"User is sending messages to Gemini: {messages}")
            
            # Create a chat session
            chat = self.model.start_chat()
            
            # Send all messages in sequence
            for message in messages:
                response = chat.send_message(message["content"])
            
            # Extract the response text
            response_text = response.text

            logger.info(f"Gemini responded with: {response_text}")
            
            # Format the response to match our expected structure
            return {
                "model": "gemini-1.5-flash",
                "message": {
                    "role": "assistant",
                    "content": response_text
                }
            }
            
        except Exception as e:
            logger.error(f"Error in Gemini chat: {str(e)}")
            raise e
