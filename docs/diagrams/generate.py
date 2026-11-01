"""Regenerate the diagram:  cd docs/diagrams && python3 generate.py"""
import fax_flow

with open("fax-flow.svg", "w", encoding="utf-8") as f:
    f.write(fax_flow.build().svg())
print("wrote fax-flow.svg")
