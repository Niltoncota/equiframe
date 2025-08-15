# app/schemas/evidence.py
from pydantic import BaseModel
from typing import Optional

try:  # Pydantic v2
    from pydantic import BaseModel, ConfigDict
    class EvidenceOut(BaseModel):
        id: int
        doc_name: str
        concept_id: int
        match_type: Optional[str] = None
        level: Optional[int] = None
        lang: Optional[str] = None
        snippet: Optional[str] = None
        pattern: Optional[str] = None
        term_or_phrase: Optional[str] = None
        model_config = ConfigDict(from_attributes=True)
except Exception:  # Pydantic v1
    from pydantic import BaseModel
    class EvidenceOut(BaseModel):
        id: int
        doc_name: str
        concept_id: int
        match_type: Optional[str] = None
        level: Optional[int] = None
        lang: Optional[str] = None
        snippet: Optional[str] = None
        pattern: Optional[str] = None
        term_or_phrase: Optional[str] = None
        class Config:
            orm_mode = True
