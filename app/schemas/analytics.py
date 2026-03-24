"""Analytics Schemas"""
from pydantic import BaseModel
from typing import List


class DashboardStats(BaseModel):
    total_applications: int
    total_sent: int
    total_opened: int
    total_replied: int
    response_rate: float
    avg_response_time_hours: float


class ApplicationsByStatus(BaseModel):
    status: str
    count: int


class ApplicationsByCompany(BaseModel):
    company_name: str
    count: int
    response_rate: float
