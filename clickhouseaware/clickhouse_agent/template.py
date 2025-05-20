"""Template rendering utilities for ClickHouse agent prompts."""

from pathlib import Path
from jinja2 import Environment, FileSystemLoader

_env = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=False,
)


def render_prompt(template_name: str, **ctx) -> str:
    """
    Renders a Jinja2 prompt template with the given context.
    """
    tpl = _env.get_template(template_name)
    return tpl.render(**ctx)
