from typing import Any, Dict, Union

class Response:
    def __init__(self, status: str, content: Any):
        self.status = status
        self.content = content

    @classmethod
    def success(cls, content: Any) -> Dict[str, Union[str, Any]]:
        return cls("success", content).to_dict()

    @classmethod
    def error(cls, content: Any) -> Dict[str, Union[str, Any]]:
        return cls("error", content).to_dict()

    def to_dict(self) -> Dict[str, Union[str, Any]]:
        return {
            "status": self.status,
            "content": self.content
        }