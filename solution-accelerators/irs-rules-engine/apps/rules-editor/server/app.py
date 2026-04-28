"""FastAPI application for IRS Rules Engine."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from server.routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
  """Manage application lifespan."""
  yield


app = FastAPI(
  title='IRS Rules Engine',
  description='DMN rule editor, test evaluation, and batch scoring for the IRS Return Review Program',
  version='1.0.0',
  lifespan=lifespan,
)

app.add_middleware(
  CORSMiddleware,
  allow_origins=['http://localhost:3000', 'http://localhost:3333', 'http://127.0.0.1:3333'],
  allow_credentials=True,
  allow_methods=['GET', 'POST'],
  allow_headers=['Content-Type'],
)

app.include_router(router, prefix='/api', tags=['api'])


@app.get('/health')
async def health():
  """Health check endpoint."""
  return {'status': 'healthy'}


# Static files — serve React build. MUST BE LAST.
if os.path.exists('client/build'):
  app.mount('/', StaticFiles(directory='client/build', html=True), name='static')
