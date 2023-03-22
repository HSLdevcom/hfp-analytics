"""HFP Analytics REST API"""
import os
from typing import Optional
import azure.functions as func
from fastapi import FastAPI, Query, Depends
from fastapi.responses import HTMLResponse
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)

from api.routers import hfp, stops, journeys, vehicles


async def verify_api_code(
    code: Optional[str] = Query(default=os.getenv("DEFAULT_AUTH_CODE", ""), description="Authentication code")
):
    pass


app = FastAPI(
    title="HSL Analytics REST API",
    description="This REST API is used to get results from analytics done with Jore-data and HFP-data. "
    "The documentation of HFP schema is here: "
    "<a href=https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/ > "
    "https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/  </a>",
    contact={"name": "HSL Analytics", "url": "https://github.com/HSLdevcom/hfp-analytics"},
    license_info={"name": "MIT License", "url": "https://github.com/HSLdevcom/hfp-analytics/blob/main/LICENSE"},
    openapi_tags=[
        {"name": "HFP data", "description": "API to query raw HFP data."},
        {"name": "Journey analytics data", "description": "API to query analytics data of journeys."},
        {"name": "Stop analytics data", "description": "API to query analytics data of stops."},
    ],
    docs_url=None,
    redoc_url=None,
    responses={
        401: {"description": "Unauthorized. API key (`code=`) was missing in request parameters."},
        500: {
            "description": "Internal server error. Something went wrong in server-side in the request. "
            "This is a bug and should be reported to developers with details how this happened."
        },
        504: {"description": "Gateway timeout. The query took too long to be accomplished."},
    },
    dependencies=[Depends(verify_api_code)],
)


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return func.AsgiMiddleware(app).handle(req, context)


# /hfp/***
app.include_router(hfp.router)
# /stops/***
app.include_router(stops.router)
# /journeys/***
app.include_router(journeys.router)
# /vehicles/***
app.include_router(vehicles.router)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(code: str = Query(default="")) -> HTMLResponse:
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    return get_swagger_ui_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " Swagger UI",
    )


@app.get("/redoc", include_in_schema=False)
async def custom_redoc_ui_html(code: str = Query(default="")) -> HTMLResponse:
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    return get_redoc_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " ReDoc",
    )
