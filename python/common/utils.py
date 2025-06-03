import pptx
import logging as logger
from datetime import date, timedelta, datetime, time

def tuples_to_feature_collection(geom_tuples: list[tuple]) -> dict:
    """Transforms GeoJSON tuples returned by psycopg into a FeatureCollection dict for REST API."""
    features = [tp[0] for tp in geom_tuples]
    return {
        "type": "FeatureCollection",
        "features": features
    }


# From https://pbpython.com/creating-powerpoint.html
def analyze_pptx(input_pptx, output_pptx):
    """ Take the input file and analyze the structure.
    The output file contains marked up information to make it easier
    for generating future powerpoint templates.
    """
    prs = pptx.Presentation(input_pptx)
    for index, _ in enumerate(prs.slide_layouts):
        slide = prs.slides.add_slide(prs.slide_layouts[index])
        try:
            title = slide.shapes.title
            title.text = 'Title for Layout {}'.format(index)
        except AttributeError:
            logger.info("No Title for Layout {}".format(index))
        for shape in slide.placeholders:
            if shape.is_placeholder:
                phf = shape.placeholder_format
                try:
                    if 'Title' not in shape.text:
                        shape.text = 'Placeholder index:{} type:{}'.format(phf.idx, shape.name)
                except AttributeError:
                    logger.info("{} has no text attribute".format(phf.type))
                logger.info('{} {}'.format(phf.idx, shape.name))
    prs.save(output_pptx)

def set_timezone(timestamp, tz_offset):
    tzone = timezone(timedelta(hours=tz_offset))
    return timestamp.replace(tzinfo=tzone)

def create_filename(prefix, *args):
    identifiers = filter(None, args)
    filename_identifier = "_".join(map(str, identifiers))
    return f"{prefix}{filename_identifier}.csv.gz"

def get_previous_day_oday(offset=1):
    today = date.today()
    yesterday = today - timedelta(days=offset)

    from_oday = yesterday.strftime("%Y-%m-%d")
    return from_oday

def get_season(month, seasons_and_months):
    key = [key for key, val in seasons_and_months.items() if month in val][0]
    return key.lower()


def is_date_range_valid(
    start_date: str | date, end_date: str | date, max_days: int = 49
) -> bool:
    """
    Check if between start_date and end_date (end_date inclusive) there are less or equal days
    than specified as max_days
    Args:
        start_date (str): start of date range as string
        end_date (str): start of date range (inclusive) as string
        max_days (int, optional): Defaults to 49 days (7 weeks).

    Returns:
        bool
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end_date < start_date:
        return False

    return ((end_date - start_date).days + 1) <= max_days
