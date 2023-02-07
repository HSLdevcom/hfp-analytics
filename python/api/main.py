"""HFP Analytics REST API"""
import azure.functions as func
from fastapi import FastAPI, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)

from api.routers import hfp, stops, journeys

app = FastAPI(
    title="HSL Analytics REST API",
    description="This REST API is used to get results from analytics done with Jore-data and HFP-data.",
    contact={
        "name": "HSL",
        "url": "https://github.com/HSLdevcom/hfp-analytics"
    },
    license_info={
        "name": "MIT License",
        "url": "https://github.com/HSLdevcom/hfp-analytics/blob/main/LICENSE"
    },
    openapi_tags=[{"name": "HFP data", "description": "API to query raw HFP data."}],
    docs_url=None,
    redoc_url=None,
)


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return func.AsgiMiddleware(app).handle(req, context)


# /hfp/***
app.include_router(hfp.router)
# /stops/***
app.include_router(stops.router)
# /journeys/***
app.include_router(journeys.router)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(request: Request):
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    code = request.query_params['code']
    return get_swagger_ui_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " Swagger UI",
    )


@app.get("/redoc", include_in_schema=False)
async def custom_redoc_ui_html(request: Request):
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    code = request.query_params['code']
    return get_redoc_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " ReDoc",
    )
