# Collect results into a pptx report.

import pptx
import psycopg2
from stopcorr.utils import get_conn_params

def main():
    conn = psycopg2.connect(**get_conn_params())

    try:
        with conn:
            with conn.cursor() as cur:
            cur.execute('SELECT vmrf.* FROM view_median_report_fields AS vmrf \
                         INNER JOIN stop_median AS sm ON (vmrf.stop_id = sm.stop_id)\
                         WHERE (sm.n_stop_known + sm.n_stop_guessed) >= 100\
                            AND (sm.dist_to_jore_point_m >= 25\
                              OR sm.manual_acceptance_needed IS true)')
            colnames = [desc[0] for desc in cur.description]
            res = [{col: row[idx] for idx, col in enumerate(colnames)} for row in cur.fetchall()]
    finally:
        conn.close()

    slide_texts = {}
    for row in res:
        slide_texts[row['stop_id']] = {phi[col]: row[col] for col in phi.keys() if col in row.keys()}

    stop_ids = sorted([row['stop_id'] for row in res])

    prs = pptx.Presentation(pptx='stopcorr_template.pptx')
    layout = prs.slide_layouts[0]
    for stop_id in stop_ids:
        slide = prs.slides.add_slide(layout)
        for k, v in slide_texts[stop_id].items():
            slide.placeholders[k].text = v or ''
    prs.save('test_out.pptx')

if __name__ == '__main__':
    main()
