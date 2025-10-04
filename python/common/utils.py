import logging as logger

import pptx


def tuples_to_feature_collection(geom_tuples: list[tuple]) -> dict:
    """Transforms GeoJSON tuples returned by psycopg into a FeatureCollection dict for REST API."""
    features = [tp[0] for tp in geom_tuples]
    return {"type": "FeatureCollection", "features": features}


# From https://pbpython.com/creating-powerpoint.html
def analyze_pptx(input_pptx, output_pptx):
    """Take the input file and analyze the structure.
    The output file contains marked up information to make it easier
    for generating future powerpoint templates.
    """
    prs = pptx.Presentation(input_pptx)
    for index, _ in enumerate(prs.slide_layouts):
        slide = prs.slides.add_slide(prs.slide_layouts[index])
        try:
            title = slide.shapes.title
            title.text = "Title for Layout {}".format(index)
        except AttributeError:
            logger.info("No Title for Layout {}".format(index))
        for shape in slide.placeholders:
            if shape.is_placeholder:
                phf = shape.placeholder_format
                try:
                    if "Title" not in shape.text:
                        shape.text = "Placeholder index:{} type:{}".format(
                            phf.idx, shape.name
                        )
                except AttributeError:
                    logger.info("{} has no text attribute".format(phf.type))
                logger.info("{} {}".format(phf.idx, shape.name))
    prs.save(output_pptx)
