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

def get_previous_day_tst():
    today = date.today()
    yesterday = today - timedelta(days=1)

    from_tst = datetime.combine(yesterday, time(0, 0, 0))
    to_tst = datetime.combine(yesterday, time(23, 59, 59))
    return from_tst.isoformat(), to_tst.isoformat()

def get_season(timestamp):
    month = timestamp.month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"