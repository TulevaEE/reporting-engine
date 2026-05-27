"""nbconvert config for blog post notebooks.

Unlike common/nbconvert_config.py (which hides input cells for stakeholder reports),
the blog version SHOWS code — the audience is data-curious readers who want to see
the analysis, data sources, and methodology.
"""
c = get_config()  # noqa: F821
c.HTMLExporter.exclude_input = False              # show code
c.TagRemovePreprocessor.enabled = True
c.TagRemovePreprocessor.remove_cell_tags = {"remove_cell"}
