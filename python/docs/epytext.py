import re
from sphinx.application import Sphinx

RULES = (
    (r"<(!BLANKLINE)[\w.]+>", r""),
    (r"L{([\w.()]+)}", r":class:`\1`"),
    (r"[LC]{(\w+\.\w+)\(\)}", r":func:`\1`"),
    (r"C{([\w.()]+)}", r":class:`\1`"),
    (r"[IBCM]{([^}]+)}", r"`\1`"),
    ("pyspark.rdd.RDD", "RDD"),
)


def _convert_epytext(line: str) -> str:
    """
    >>> _convert_epytext("L{A}")
    :class:`A`
    """
    line = line.replace("@", ":")
    for p, sub in RULES:
        line = re.sub(p, sub, line)
    return line


def _process_docstring(
    app: "Sphinx", what: str, name: str, obj: object, options: dict, lines: list[str]
) -> None:
    for i in range(len(lines)):
        lines[i] = _convert_epytext(lines[i])


def setup(app: "Sphinx") -> None:
    app.connect("autodoc-process-docstring", _process_docstring)
