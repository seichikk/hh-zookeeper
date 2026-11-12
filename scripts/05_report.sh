#!/bin/bash
source "$(dirname "$0")/common.sh"

for f in Coordinator.take_epoch Coordinator.lead Coordinator.checkpoint Coordinator.publish Master.collect slow_map; do
  py -c "import inspect, textwrap, lab.coordinator as m; print(textwrap.dedent(inspect.getsource(m.$f)).rstrip())" \
    > "$OUT/code_${f/./_}.txt" || exit 1
done

PDFLATEX=$(type -P pdflatex || echo /Library/TeX/texbin/pdflatex)
cd report || exit 1
for i in 1 2; do
  "$PDFLATEX" -interaction=nonstopmode -halt-on-error report.tex >/dev/null 2>&1 || {
    grep -A5 '^!' report.log | head -20
    exit 1
  }
done
ls -lh report.pdf
