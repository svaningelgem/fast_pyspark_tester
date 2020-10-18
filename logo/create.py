"""Creates an SVG of the Databench logo. Optionally also a png."""

import os
import random
from pathlib import Path

import svgwrite

DATA = [
    [0, 1, 1, 1, 1, 1, 1, 1],
    [0, 1, 1, 1, 1, 1, 1, 1],
    [0, 0, 0, 0, 1, 1, 1, 1],
    [0, 0, 0, 1, 1, 1, 1, 1],
    [0, 0, 1, 1, 1, 0, 1, 1],
    [0, 1, 1, 1, 0, 0, 1, 1],
    [1, 1, 1, 0, 0, 0, 1, 1],
    [1, 1, 0, 0, 0, 0, 0, 0],
]


def color(x, y):
    """triangles.

    Colors:
    - http://paletton.com/#uid=70l150klllletuehUpNoMgTsdcs shade 2
    """

    return '#42359C'  # "#CDB95B"

    if (x-4) > (y-4) and -(y-4) <= (x-4):
        # right
        return '#42359C'  # "#CDB95B"
    elif (x-4) > (y-4) and -(y-4) > (x-4):
        # top
        return "#CD845B"
    elif (x-4) <= (y-4) and -(y-4) <= (x-4):
        # bottom
        return "#57488E"
    elif (x-4) <= (y-4) and -(y-4) > (x-4):
        # left
        return "#3B8772"

    # should not happen
    return "black"


def simple(svg_document, x, y, v):
    if v == 1:
        svg_document.add(svg_document.rect(insert=(x*16, y*16),
                                           size=("16px", "16px"),
                                           # rx="2px",
                                           # stroke_width="1",
                                           # stroke=color(x, y),
                                           fill=color(x, y)))


def smaller(svg_document, x, y, v, x_offset=0, y_offset=0):
    # from center
    distance2 = (x-3.5)**2 + (y-3.5)**2
    max_distance2 = 2 * 4**2

    if v == 1:
        size = 16.0*(1.0 - distance2/max_distance2)
        number_of_cubes = int(16**2 / (size**2))
        for i in range(number_of_cubes):
            xi = x*16 + 1 + random.random()*(14.0-size) + x_offset
            yi = y*16 + 1 + random.random()*(14.0-size) + y_offset
            sizepx = str(size)+"px"
            svg_document.add(svg_document.rect(insert=(xi, yi),
                                               size=(sizepx, sizepx),
                                               rx="2px",
                                               stroke_width="1",
                                               # stroke='#4E9954',
                                               stroke='#FAE5A5',
                                               # stroke=color(x, y),
                                               fill=color(x, y)))


def main():
    root = Path(__file__).parent
    svg_favicon = svgwrite.Drawing(filename=root/"favicon.svg",
                                   size=("128px", "128px"))
    svg_document = svgwrite.Drawing(filename=root/"logo.svg",
                                    size=("128px", "128px"))
    svg_banner = svgwrite.Drawing(filename=root/"banner.svg",
                                  size=("600px", "200px"))
    for y, r in enumerate(DATA):
        for x, v in enumerate(r):
            simple(svg_favicon, x, y, v)
            smaller(svg_document, x, y, v)
            smaller(svg_banner, x, y, v, x_offset=20, y_offset=40)
    # add banner text
    g = svg_banner.g(style='font-size:40px; font-family:Arial; font-weight: bold; font-style: italic;')
    g.add(svg_banner.text(
        'fast_pyspark_tester',
        insert=(180, 120), fill='#000000'),
    )
    svg_banner.add(g)
    # print(svg_document.tostring())
    svg_favicon.save(pretty=True)
    svg_document.save(pretty=True)
    svg_banner.save(pretty=True)

    # create pngs
    os.system(f'svg2png --width=100 --height=100 "{svg_document.filename}" --output="logo-w100.png"')
    os.system(f'svg2png --width=600 --height=600 "{svg_document.filename}" --output="logo-w600.png"')
    os.system(f'svg2png --width=500 --height=100 "{svg_banner.filename}" --output="banner-w500.png"')
    os.system(f'svg2png --width=1500 --height=400 "{svg_banner.filename}" --output="banner-w1500.png"')
    favicon_sizes = [16, 32, 48, 128, 256]
    for s in favicon_sizes:
        os.system(f'svg2png --width={s} --height={s} "{svg_favicon.filename}" --output="favicon-w{s}.png"')
    png_favicon_names = [f'favicon-w{s}.png' for s in favicon_sizes]
    os.system('convert ' + (' '.join(png_favicon_names)) +
              ' -colors 256 favicon.ico')


if __name__ == "__main__":
    random.seed(42)
    main()
