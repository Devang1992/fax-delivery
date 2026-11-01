"""Tiny SVG toolkit for the flow diagrams (dark theme). Regenerate with: python3 generate.py"""
from xml.sax.saxutils import escape

FONT = "Segoe UI, Inter, Helvetica Neue, Arial, sans-serif"
BG = "#17181d"

KINDS = {
    #            fill       stroke     text
    "process":  ("#1d3527", "#4caf6a", "#e3f4e8"),
    "decision": ("#2a2342", "#9b87d6", "#ece7fb"),
    "error":    ("#3b1c27", "#e0637f", "#fbe3ea"),
    "breaker":  ("#3b2717", "#e8913f", "#fdebd8"),
    "store":    ("#262b33", "#9aa6b6", "#e7ebf0"),
    "kafka":    ("#16273b", "#4f8fd6", "#dbe9f8"),
    "log":      ("#14333a", "#3fb6c2", "#d9f3f5"),
    "external": ("#1a2a40", "#6aa9f0", "#dcebfd"),
    "retry":    ("#332d14", "#d8bd4c", "#f8f0cf"),
    "note":     ("#202228", "#4b5160", "#c3c8d2"),
}

EDGE = {
    "normal":  ("#a3acba", None),
    "error":   ("#e0637f", None),
    "retry":   ("#d8bd4c", "6,4"),
    "breaker": ("#e8913f", "6,4"),
    "async":   ("#7f8a9b", "4,4"),
    "ok":      ("#4caf6a", None),
}


class Diagram:
    def __init__(self, width, height, title, subtitle=""):
        self.w, self.h = width, height
        self.parts = []
        self.labels = []
        self.nodes = {}
        self.parts.append(f'<rect width="{width}" height="{height}" fill="{BG}"/>')
        self.text(40, 46, title, 26, "#f2f4f8", weight="700")
        if subtitle:
            self.text(40, 74, subtitle, 14, "#9aa3b2")

    # ------------------------------------------------------------ primitives
    def text(self, x, y, s, size=13, color="#e6e9ef", anchor="start", weight="400", italic=False):
        style = ' font-style="italic"' if italic else ""
        self.parts.append(
            f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" fill="{color}" '
            f'text-anchor="{anchor}" font-weight="{weight}"{style}>{escape(s)}</text>')

    def _lines(self, cx, cy, txt, color, size, first_bold=True):
        lines = txt.split("\n")
        lh = size * 1.32
        y0 = cy - (len(lines) - 1) * lh / 2 + size * 0.35
        for i, line in enumerate(lines):
            w = "700" if (i == 0 and first_bold) else "400"
            sz = size if i == 0 else size - 1
            self.text(cx, y0 + i * lh, line, sz, color, "middle", w)

    def _register(self, nid, x, y, w, h):
        if nid:
            self.nodes[nid] = dict(x=x, y=y, w=w, h=h, cx=x + w / 2, cy=y + h / 2,
                                   l=(x, y + h / 2), r=(x + w, y + h / 2), t=(x + w / 2, y), b=(x + w / 2, y + h))

    def box(self, nid, x, y, w, h, txt, kind="process", size=13, first_bold=True):
        fill, stroke, tc = KINDS[kind]
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" '
                          f'stroke="{stroke}" stroke-width="1.6"/>')
        self._lines(x + w / 2, y + h / 2, txt, tc, size, first_bold)
        self._register(nid, x, y, w, h)

    def pill(self, nid, x, y, w, h, txt, kind="log", size=13):
        fill, stroke, tc = KINDS[kind]
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{h/2}" fill="{fill}" '
                          f'stroke="{stroke}" stroke-width="1.8"/>')
        self._lines(x + w / 2, y + h / 2, txt, tc, size)
        self._register(nid, x, y, w, h)

    def diamond(self, nid, cx, cy, w, h, txt, kind="decision", size=12):
        fill, stroke, tc = KINDS[kind]
        pts = f"{cx},{cy - h/2} {cx + w/2},{cy} {cx},{cy + h/2} {cx - w/2},{cy}"
        self.parts.append(f'<polygon points="{pts}" fill="{fill}" stroke="{stroke}" stroke-width="1.6"/>')
        self._lines(cx, cy, txt, tc, size)
        self._register(nid, cx - w / 2, cy - h / 2, w, h)

    def cylinder(self, nid, x, y, w, h, txt, kind="store", size=13):
        fill, stroke, tc = KINDS[kind]
        ry = min(12, h * 0.15)
        self.parts.append(
            f'<path d="M{x},{y+ry} A{w/2},{ry} 0 0 1 {x+w},{y+ry} L{x+w},{y+h-ry} A{w/2},{ry} 0 0 1 {x},{y+h-ry} Z" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1.6"/>')
        self.parts.append(f'<ellipse cx="{x+w/2}" cy="{y+ry}" rx="{w/2}" ry="{ry}" fill="{fill}" '
                          f'stroke="{stroke}" stroke-width="1.6"/>')
        self._lines(x + w / 2, y + h / 2 + ry / 2, txt, tc, size)
        self._register(nid, x, y, w, h)

    def group(self, x, y, w, h, title, color="#5b6272", title_color="#aab2c0"):
        self.parts.insert(1, f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#1b1d23" '
                             f'stroke="{color}" stroke-width="1.3" stroke-dasharray="7,5"/>')
        self.parts.insert(2, f'<text x="{x+14}" y="{y+22}" font-family="{FONT}" font-size="13" '
                             f'fill="{title_color}" font-weight="600">{escape(title)}</text>')

    def band(self, x, y, w, h, title, kind):
        fill, stroke, tc = KINDS[kind]
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" '
                          f'stroke="{stroke}" stroke-width="1.3"/>')
        self.text(x + w / 2, y + h / 2 + 5, title, 13, tc, "middle", "600")

    def note(self, x, y, lines, size=12, color="#b9c0cc", lh=None):
        """Lines may contain **bold** segments."""
        lh = lh or size * 1.4
        for i, line in enumerate(lines):
            segs = line.split("**")
            spans = []
            for j, seg in enumerate(segs):
                if not seg:
                    continue
                bold = j % 2 == 1
                spans.append(f'<tspan fill="{"#e6e9ef" if bold else color}" '
                             f'font-weight="{"700" if bold else "400"}">{escape(seg)}</tspan>')
            self.parts.append(f'<text x="{x}" y="{y + i * lh}" font-family="{FONT}" font-size="{size}" '
                              f'xml:space="preserve">{"".join(spans)}</text>')

    # ------------------------------------------------------------ edges
    def arrow(self, pts, label=None, kind="normal", lpos=None, lanchor="middle", head=True):
        color, dash = EDGE[kind]
        d = "M" + " L".join(f"{x},{y}" for x, y in pts)
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        marker = f' marker-end="url(#ah-{kind})"' if head else ""
        self.parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.7"{dash_attr}{marker}/>')
        if label:
            if lpos is None:
                (x1, y1), (x2, y2) = pts[0], pts[1]
                lpos = ((x1 + x2) / 2, (y1 + y2) / 2 - 6)
            self.labels.append((lpos, label, color, lanchor))

    def _label(self, pos, label, color, anchor):
        x, y = pos
        lines = label.split("\n")
        size = 11.5
        lh = size * 1.3
        width = max(len(l) for l in lines) * size * 0.56 + 10
        height = len(lines) * lh + 4
        if anchor == "middle":
            rx = x - width / 2
        elif anchor == "start":
            rx = x - 5
        else:
            rx = x - width + 5
        out = [f'<rect x="{rx}" y="{y - size - 1}" width="{width}" height="{height}" rx="4" fill="{BG}" '
               f'fill-opacity="0.92"/>']
        for i, line in enumerate(lines):
            out.append(f'<text x="{x}" y="{y + i*lh}" font-family="{FONT}" font-size="{size}" fill="{color}" '
                       f'text-anchor="{anchor}" font-weight="600">{escape(line)}</text>')
        return "".join(out)

    def legend(self, x, y):
        items = [("process", "processing step"), ("decision", "decision"), ("external", "HTTP call"),
                 ("error", "terminal failure (ERROR)"), ("log", "success / outcome log"),
                 ("retry", "retry / deferred"), ("breaker", "circuit breaker"), ("store", "state / Kafka")]
        self.text(x, y + 4, "Legend", 13, "#d6dbe4", weight="700")
        cx = x + 70
        for kind, label in items:
            fill, stroke, _ = KINDS[kind]
            self.parts.append(f'<rect x="{cx}" y="{y-10}" width="22" height="16" rx="3" fill="{fill}" '
                              f'stroke="{stroke}" stroke-width="1.5"/>')
            self.text(cx + 30, y + 3, label, 12, "#b9c0cc")
            cx += 38 + len(label) * 6.9
        for kind, label in [("normal", "flow"), ("error", "failure"), ("retry", "retry / loop"),
                            ("breaker", "breaker event"), ("async", "store read/write")]:
            color, dash = EDGE[kind]
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            self.parts.append(f'<path d="M{cx},{y-2} L{cx+34},{y-2}" stroke="{color}" stroke-width="2"{dash_attr} '
                              f'marker-end="url(#ah-{kind})"/>')
            self.text(cx + 42, y + 3, label, 12, "#b9c0cc")
            cx += 56 + len(label) * 6.9

    def svg(self):
        defs = ["<defs>"]
        for kind, (color, _) in EDGE.items():
            defs.append(f'<marker id="ah-{kind}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                        f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
                        f'fill="{color}"/></marker>')
        defs.append("</defs>")
        labels = [self._label(*l) for l in self.labels]
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
                f'viewBox="0 0 {self.w} {self.h}">' + "".join(defs) + "".join(self.parts) + "".join(labels)
                + "</svg>")
