"""Fraud Rules Engine API routes."""

from fastapi import APIRouter

from server.routers.catalog import router as catalog_router
from server.routers.dmn import router as dmn_router
from server.routers.scoring import router as scoring_router

router = APIRouter()
router.include_router(dmn_router, prefix='/dmn', tags=['dmn'])
router.include_router(scoring_router, prefix='/scoring', tags=['scoring'])
router.include_router(catalog_router, prefix='/catalog', tags=['catalog'])
