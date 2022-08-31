import os
import pptx
# from common.logger_util import get_logger

def get_conn_params():
    return dict(
        dbname = os.getenv('POSTGRES_DB'),
        user = os.getenv('POSTGRES_USER'),
        password = os.getenv('POSTGRES_PASSWORD'),
        host = os.getenv('POSTGRES_HOST'),
        port = 5432
    )

def env_with_default(var_name, default_value):
    res = os.getenv(var_name)
    if res is None:
        res = default_value
        print(f'{var_name} not set, falling back to default value {default_value}')
    return res

def get_feature_collection(features):
    return {
        "type": "FeatureCollection",
        "features": features
    }

def comma_separated_floats_to_list(val_str):
    res = val_str.split(',')
    res = [x.strip() for x in res]
    res = [float(x) for x in res]
    return res

def comma_separated_integers_to_list(val_str):
    res = val_str.split(',')
    res = [x.strip() for x in res]
    res = [int(x) for x in res]
    return res

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
            print("TODO:")
            # logger.info("No Title for Layout {}".format(index))
        for shape in slide.placeholders:
            if shape.is_placeholder:
                phf = shape.placeholder_format
                try:
                    if 'Title' not in shape.text:
                        shape.text = 'Placeholder index:{} type:{}'.format(phf.idx, shape.name)
                except AttributeError:
                    print("TODO:")
                    # logger.info("{} has no text attribute".format(phf.type))
                # logger.info('{} {}'.format(phf.idx, shape.name))
    prs.save(output_pptx)
