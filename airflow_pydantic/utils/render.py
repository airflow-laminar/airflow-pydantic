from typing import List, Tuple

__all__ = ("RenderedCode",)

Imports = List[str]
Globals = List[str]
InsideDag = List[str]

RenderedCode = Tuple[Imports, Globals, InsideDag]
