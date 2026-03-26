from pydantic import BaseModel
from typing import List

class Recommendation(BaseModel):
    rank: int
    h3_cell: str
    lat: float
    lon: float
    predicted_demand: float
    charger_score: float
    grid_distance: int
    score: float

class RecommendationResponse(BaseModel):
    driver_h3: str
    hour: int
    day_of_week: int
    recommendations: List[Recommendation]