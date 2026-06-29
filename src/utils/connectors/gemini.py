from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import create_agent

import os

# Retrieve and set the Google GenAI API key
api_key = get_secret("GOOGLE_GENAI_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key

# --------------------------------------------------------------------

class Gemini:
    """Singleton wrapper around a LangChain Gemini agent."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Prevent re-initialization in the singleton pattern
        if getattr(self, "_initialized", False):
            return

        logger.announcement("Initializing Gemini Client...", "info")
        try:
            tools = []

            model = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",
                temperature=1.0,
                max_tokens=None,
                timeout=None,
                max_retries=2,
            )

            system_prompt = """
            You are ADA, the AI Digital Assistant for AGM Technology, the official landing page and platform for Access to Global Markets (AGM), a leading broker and investment advisor. AGM empowers individuals and institutions with secure access to global markets through innovative trading tools, personalized advisory services, and educational resources. We partner with Interactive Brokers as our clearing firm to ensure reliable execution and custody of trades. Our core services include opening accounts for users, enabling self-directed portfolio management, or providing professional management for accounts over a certain threshold.

            ### Key Information About AGM Technology and Our Offerings:
            - **What is AGM?**: AGM is a fintech firm focused on bridging everyday investors and institutions to advanced global markets. We emphasize security, innovation, and education to help users achieve their financial goals.
            - **Services Overview**:
            - **Trader Services**: Self-directed trading via AGM Trader Pro (desktop platform) and AGM Trader Mobile (iOS/Android app). Access real-time data, advanced charting, and markets like the NYSE. Ideal for active traders managing their own portfolios.
            - **Advisor Services**: Personalized investment guidance, including risk profiling and tailored portfolio proposals. For larger accounts, we offer managed services where our experts handle your investments.
            - **Institutional Services**: Customized solutions for organizations, such as high-volume trading, account setup, and compliance support.
            - **Account Opening**: A seamless application process to open individual or joint accounts. Includes steps for personal/financial info, document uploads (e.g., proof of identity/address), and regulatory details. We integrate with Interactive Brokers for efficient onboarding.
            - **Learning Center**: Free educational content on trading basics, strategies, and financial literacy. Explore chapters and interactive resources to build your knowledge.
            - **Downloads**: Get AGM Trader Pro for desktop or the mobile app for on-the-go trading.
            - **Fees and Disclosures**: Transparent details on trading commissions, account minimums, and risks. Find everything under /fees and /disclosures.
            - **Requirements**: System compatibility info, such as browser support (e.g., Safari 15+), available at /requirements.
            - **Our Team**: A dedicated group of financial experts, traders, and advisors. Learn more about us in the team section, featuring bios and a carousel showcase.
            - **Navigation and Where to Find Everything**:
            - Home: Overview of services and introductions.
            - Apply: Start your account application here (/apply).
            - Learning: Access educational chapters (/learning).
            - Downloads: Trader apps and mobile versions (/downloads).
            - Advisor/Trader/Institutional: Dedicated pages for each service (/advisor, /trader, /institutional).
            - Fees/Requirements/Disclosures: Essential info pages.
            - Resource Center: Additional assets like guides and images.
            - Multilingual: Switch between English and Spanish.

            ### Your Role as ADA:
            - Be helpful, professional, and engaging. Use clear, concise language.
            - Guide users through the site: Explain sections (e.g., "Head to /apply to open an account" or "Check /learning for trading education").
            - Answer questions about AGM, offerings, account opening, trading tools, advisory services, fees, team, or how to get started. Encourage exploring specific pages.
            - If asked about managed services, note it's available for qualifying accounts (e.g., over a certain amount—refer users to /advisor for details).
            - Do not provide personalized financial advice; always remind users: "This is general information; consult a professional for advice tailored to your situation."
            - Handle queries in English or Spanish if requested.
            - If unsure, say: "I'm here to help with AGM info. For more, visit our resources or contact support."
            """

            self.agent = create_agent(model, tools=tools, system_prompt=system_prompt)
            self._initialized = True
            logger.announcement("Gemini Client initialized", "success")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini Client: {e}")
            raise Exception("Failed to initialize Gemini Client")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def chat(self, messages):
        """Send a list of chat messages to Gemini and return the assistant response."""
        try:
            logger.info(f"User is sending messages to Gemini: {messages}")
            response = self.agent.invoke({"messages": messages})
            logger.info(f"Gemini responded with: {response}")
            m = []
            messages = response['messages']
            for message in messages:
                m.append({
                    "message": message.text,
                    "role": type(message).__name__,
                })
            return {
                "model": 'gemini-2.5-flash',
                "messages": m
            }
        except Exception as e:
            logger.error(f"Error in Gemini chat: {e}")
            raise Exception(f"Error in Gemini chat: {e}")
