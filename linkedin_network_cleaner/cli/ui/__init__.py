"""CLI UI components — Rich-based display helpers."""

from .console import (
    console,
    print_header,
    print_subheader,
    print_divider,
    print_success,
    print_step,
    print_suggested_next,
    print_funnel,
    print_breakdown,
    print_sample_row,
    print_metric_line,
)
from .errors import show_error, show_warning, show_info
from .progress import (
    create_extraction_progress,
    create_enrichment_progress,
    create_scoring_progress,
    create_action_progress,
    create_simple_progress,
)
from .tables import (
    make_status_table,
    make_extract_status_table,
    make_pipeline_status_table,
    make_cleanup_preview_table,
    make_sample_table,
    make_summary_table,
)
from . import theme

__all__ = [
    "console",
    "print_header",
    "print_subheader",
    "print_divider",
    "print_success",
    "print_step",
    "print_suggested_next",
    "print_funnel",
    "print_breakdown",
    "print_sample_row",
    "print_metric_line",
    "show_error",
    "show_warning",
    "show_info",
    "create_extraction_progress",
    "create_enrichment_progress",
    "create_scoring_progress",
    "create_action_progress",
    "create_simple_progress",
    "make_status_table",
    "make_extract_status_table",
    "make_pipeline_status_table",
    "make_cleanup_preview_table",
    "make_sample_table",
    "make_summary_table",
    "theme",
]
