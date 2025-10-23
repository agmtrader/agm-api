from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_core.vectorstores import InMemoryVectorStore
from langchain.agents import create_agent
from langchain.tools import tool

#from langchain_community.document_loaders import WebBaseLoader
#from langchain_text_splitters import RecursiveCharacterTextSplitter

import os

# Retrieve and set the Google GenAI API key
api_key = get_secret("GOOGLE_GENAI_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key

# ------- Optional Retrieval Helpers (Embeddings & VectorStore) -------
embeddings = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001")
vector_store = InMemoryVectorStore(embeddings)

@tool(response_format="content_and_artifact")
def retrieve_context(query: str):
    """Retrieve relevant context chunks from the in-memory vector store."""
    retrieved_docs = vector_store.similarity_search(query, k=2)
    serialized = "\n\n".join(
        (f"Source: {doc.metadata}\nContent: {doc.page_content}") for doc in retrieved_docs
    )
    return serialized, retrieved_docs

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
            """
            loader = WebBaseLoader(
                web_paths=("https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-account-management",)
            )
            docs = loader.load()
            logger.info(f"Total docs: {len(docs)}")
            
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000,  # chunk size (characters)
                chunk_overlap=200,  # chunk overlap (characters)
                add_start_index=True,  # track index in original document
            )
            all_splits = text_splitter.split_documents(docs)
            logger.info(f"Total splits: {len(all_splits)}")
            vector_store.add_documents(documents=all_splits)

            tools = [retrieve_context]
            """
            tools = []

            model = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",
                temperature=0,
                max_tokens=None,
                timeout=None,
                max_retries=2,
            )

            self.agent = create_agent(model, tools=tools)
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
            return {
                "model": "gemini-2.5-flash",
                "message": {
                    "role": "assistant",
                    "content": response["messages"][-1].content,
                },
            }
        except Exception as e:
            logger.error(f"Error in Gemini chat: {e}")
            raise Exception(f"Error in Gemini chat: {e}")
