from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime


class AIRequest(BaseModel):
    """Base schema for AI requests"""
    prompt: str
    context: Optional[Dict[str, Any]] = None
    model: Optional[str] = "gpt-3.5-turbo"
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000


class AIResponse(BaseModel):
    """Base schema for AI responses"""
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None
    usage: Optional[Dict[str, int]] = None
    timestamp: datetime


class ChatMessage(BaseModel):
    """Schema for chat messages"""
    role: str  # "user", "assistant", "system"
    content: str
    timestamp: Optional[datetime] = None


class ChatRequest(BaseModel):
    """Schema for chat requests"""
    messages: List[ChatMessage]
    model: Optional[str] = "gpt-3.5-turbo"
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000


class ChatResponse(BaseModel):
    """Schema for chat responses"""
    status: str
    message: str
    response: Optional[str] = None
    usage: Optional[Dict[str, int]] = None
    timestamp: datetime


class AnalysisRequest(BaseModel):
    """Schema for data analysis requests"""
    data_type: str  # "loan", "extradana", "general"
    analysis_type: str  # "summary", "trends", "insights", "recommendations"
    filters: Optional[Dict[str, Any]] = None
    date_range: Optional[Dict[str, str]] = None


class AnalysisResponse(BaseModel):
    """Schema for analysis responses"""
    status: str
    message: str
    analysis: Optional[str] = None
    insights: Optional[List[str]] = None
    recommendations: Optional[List[str]] = None
    data_summary: Optional[Dict[str, Any]] = None
    timestamp: datetime
